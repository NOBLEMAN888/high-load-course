package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        // Processing time is not constant, so the completion estimate is inflated a bit
        private const val ESTIMATE_SAFETY_FACTOR = 1.1

        private const val MIN_LATENCY_SAMPLES = 50
        private const val COLD_START_LATENCY_FACTOR = 1.5
        private const val LATENCY_ESTIMATE_REFRESH_MS = 200
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private data class PaymentTask(
        val paymentId: UUID,
        val amount: Int,
        val startedAt: Long,
        val deadline: Long,
        val transactionId: UUID = UUID.randomUUID(),
    )

    private val queue = DeadlinePaymentQueue<PaymentTask>(rateLimitPerSec, parallelRequests)
    private val dispatcher = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory("payment-dispatch-$accountName"))
    private val submissionWorkers = Executors.newFixedThreadPool(
        minOf(32, parallelRequests), NamedThreadFactory("payment-submit-$accountName"))
    private val sendLock = Any()
    private val sendIntervalNanos = (1_000_000_000L + rateLimitPerSec - 1) / rateLimitPerSec + 1_000_000L
    private var nextSendNanos = System.nanoTime()

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 5, TimeUnit.MINUTES))
        .build()

    private val rejectedByCapacity = meterRegistry.counter("payment.queue.rejected", "account", accountName, "reason", "capacity")
    private val rejectedByDeadline = meterRegistry.counter("payment.queue.rejected", "account", accountName, "reason", "deadline")
    private val expiredInQueue = meterRegistry.counter("payment.queue.expired", "account", accountName)
    private val sent = meterRegistry.counter("payment.provider.sent", "account", accountName)
    private val queueWait = Timer.builder("payment.queue.wait").tag("account", accountName).register(meterRegistry)

    private val providerLatency = Timer.builder("payment.provider.duration")
        .tag("account", accountName)
        .publishPercentiles(0.95)
        .distributionStatisticExpiry(Duration.ofSeconds(30))
        .register(meterRegistry)

    @Volatile
    private var processingEstimateMs = (requestAverageProcessingTime.toMillis() * COLD_START_LATENCY_FACTOR).toLong()

    @Volatile
    private var processingEstimateUpdatedAt = 0L

    private fun processingEstimateMs(): Long {
        val now = now()
        if (now - processingEstimateUpdatedAt > LATENCY_ESTIMATE_REFRESH_MS) {
            processingEstimateUpdatedAt = now
            val p95 = providerLatency.takeSnapshot().percentileValues().firstOrNull()?.value(TimeUnit.MILLISECONDS)
            if (providerLatency.count() >= MIN_LATENCY_SAMPLES && p95 != null && p95 > 0) {
                processingEstimateMs = maxOf(requestAverageProcessingTime.toMillis(), p95.toLong())
            }
        }
        return processingEstimateMs
    }

    init {
        Gauge.builder("payment.queue.size", queue) { it.waitingCount().toDouble() }
            .tag("account", accountName).register(meterRegistry)
        Gauge.builder("payment.provider.active", queue) { it.activeCount().toDouble() }
            .tag("account", accountName).register(meterRegistry)
        dispatcher.scheduleWithFixedDelay({
            try {
                dispatchNext()
            } catch (e: Exception) {
                logger.error("[$accountName] Payment dispatch failed", e)
            }
        }, 0, 2, TimeUnit.MILLISECONDS)
    }

    private fun completionBudgetMs() =
        (processingEstimateMs() * ESTIMATE_SAFETY_FACTOR).toLong().coerceAtLeast(1)

    override fun tryReserve(deadline: Long): Boolean {
        val rejection = queue.reserve(deadline, completionBudgetMs())
        when (rejection) {
            DeadlinePaymentQueue.Rejection.CAPACITY -> rejectedByCapacity.increment()
            DeadlinePaymentQueue.Rejection.DEADLINE -> rejectedByDeadline.increment()
            null -> return true
        }
        return false
    }

    override fun releaseReservation() = queue.releaseReservation()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        queue.enqueue(PaymentTask(paymentId, amount, paymentStartedAt, deadline), deadline)
    }

    private fun dispatchNext() {
        val dispatch = queue.poll(completionBudgetMs()) ?: return
        try {
            submissionWorkers.execute { submit(dispatch) }
            if (!dispatch.expired) queue.dispatched()
        } catch (e: Exception) {
            if (!dispatch.expired) queue.complete(dispatch.id)
            throw e
        }
    }

    private fun submit(dispatch: DeadlinePaymentQueue.Dispatch<PaymentTask>) {
        val task = dispatch.task
        var handedOff = false
        try {
            queueWait.record(maxOf(0, now() - task.startedAt), TimeUnit.MILLISECONDS)
            paymentESService.update(task.paymentId) {
                it.logSubmission(!dispatch.expired, task.transactionId, now(), Duration.ofMillis(now() - task.startedAt))
            }
            if (dispatch.expired) {
                expire(task)
                return
            }
            // Persistence above can take time. Recheck immediately before the HTTP call.
            if (task.deadline - now() < completionBudgetMs()) {
                expire(task)
                return
            }
            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${task.transactionId}&paymentId=${task.paymentId}&amount=${task.amount}")
                .post(emptyBody)
                .build()
            val call = client.newCall(request)
            call.timeout().timeout(maxOf(1, task.deadline - now()), TimeUnit.MILLISECONDS)
            var sentAt = 0L
            val callback = object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    try {
                        // Fast rate-limit errors must not lower the processing-time estimate.
                        if (response.isSuccessful) providerLatency.record(now() - sentAt, TimeUnit.MILLISECONDS)
                        response.use { handleResponse(it, task.transactionId, task.paymentId) }
                    } catch (e: Exception) {
                        handleFailure(e, task.transactionId, task.paymentId)
                    } finally {
                        queue.complete(dispatch.id)
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    try {
                        handleFailure(e, task.transactionId, task.paymentId)
                    } finally {
                        queue.complete(dispatch.id)
                    }
                }
            }
            synchronized(sendLock) {
                val remaining = nextSendNanos - System.nanoTime()
                if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining)
                if (task.deadline - now() < completionBudgetMs()) {
                    expire(task)
                    return
                }
                sentAt = now()
                call.enqueue(callback)
                handedOff = true
                nextSendNanos = System.nanoTime() + sendIntervalNanos
                sent.increment()
            }
        } catch (e: Exception) {
            handleFailure(e, task.transactionId, task.paymentId)
        } finally {
            if (!dispatch.expired && !handedOff) queue.complete(dispatch.id)
        }
    }

    private fun expire(task: PaymentTask) {
        expiredInQueue.increment()
        paymentESService.update(task.paymentId) {
            it.logProcessing(false, now(), task.transactionId, reason = "Payment deadline exceeded before dispatch.")
        }
    }

    override fun close() {
        dispatcher.shutdown()
        submissionWorkers.shutdown()
        client.dispatcher.cancelAll()
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }

    private fun handleResponse(response: Response, transactionId: UUID, paymentId: UUID) {
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    private fun handleFailure(e: Exception, transactionId: UUID, paymentId: UUID) {
        when (e) {
            is InterruptedIOException -> {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
