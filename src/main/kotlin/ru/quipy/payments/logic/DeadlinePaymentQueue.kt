package ru.quipy.payments.logic

import java.util.ArrayDeque
import java.util.PriorityQueue

internal class DeadlinePaymentQueue<T>(
    rate: Int,
    private val parallelism: Int,
    private val capacity: Int = 1_000,
    private val clock: () -> Long = System::currentTimeMillis,
    private val monotonicMillis: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    init {
        require(rate > 0 && parallelism > 0 && capacity > 0)
    }

    private val intervalMs = (1_000L + rate - 1) / rate + 1
    private val tasks = ArrayDeque<Pending<T>>()
    private val active = mutableMapOf<Long, Long>()
    private var waiting = 0
    private var sequence = 0L
    private var nextStart = monotonicMillis()

    enum class Rejection { CAPACITY, DEADLINE }
    private data class Pending<T>(val task: T, val deadline: Long)
    data class Dispatch<T>(val task: T, val id: Long, val expired: Boolean)

    @Synchronized
    fun reserveByCapacity(limit: Int): Boolean {
        if (waiting >= minOf(capacity, limit)) return false
        waiting++
        return true
    }

    @Synchronized
    fun reserve(deadline: Long, processingMs: Long): Rejection? = reserve(deadline, processingMs, capacity)

    @Synchronized
    fun reserve(deadline: Long, processingMs: Long, maxWaiting: Int): Rejection? {
        if (waiting >= minOf(capacity, maxWaiting)) return Rejection.CAPACITY
        val now = monotonicMillis()
        val slots = PriorityQueue<Long>()
        active.values.forEach { started ->
            slots.add(if (started + processingMs > now) started + processingMs else now + processingMs)
        }
        repeat(parallelism - active.size) { slots.add(now) }
        var start = maxOf(now, nextStart)
        var completion = now
        repeat(waiting + 1) {
            start = maxOf(start, slots.remove())
            completion = start + processingMs
            slots.add(completion)
            start += intervalMs
        }
        if (completion - now > deadline - clock()) return Rejection.DEADLINE
        waiting++
        return null
    }

    @Synchronized
    fun releaseReservation() {
        check(waiting > tasks.size) { "No unsubmitted reservation" }
        waiting--
    }

    @Synchronized
    fun enqueue(task: T, deadline: Long) {
        check(waiting > tasks.size) { "A reservation is required" }
        tasks.addLast(Pending(task, deadline))
    }

    @Synchronized
    fun poll(processingMs: Long): Dispatch<T>? {
        val head = tasks.peekFirst() ?: return null
        val expired = head.deadline - clock() < processingMs
        val now = monotonicMillis()
        if (!expired && (active.size >= parallelism || now < nextStart)) return null
        tasks.removeFirst()
        waiting--
        val id = sequence++
        if (!expired) {
            active[id] = now
            nextStart = now + intervalMs
        }
        return Dispatch(head.task, id, expired)
    }

    @Synchronized
    fun dispatched() {
        nextStart = monotonicMillis() + intervalMs
    }

    @Synchronized
    fun complete(id: Long) {
        check(active.remove(id) != null) { "Unknown active payment" }
    }

    @Synchronized
    fun waitingCount(): Int = waiting

    @Synchronized
    fun activeCount(): Int = active.size
}
