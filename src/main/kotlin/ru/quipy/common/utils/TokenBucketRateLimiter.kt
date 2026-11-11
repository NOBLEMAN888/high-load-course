package ru.quipy.common.utils

import java.util.concurrent.TimeUnit

/** Allows a bounded burst and replenishes permits continuously using monotonic time. */
class TokenBucketRateLimiter(
    rate: Int,
    bucketMaxCapacity: Int,
    window: Long,
    timeUnit: TimeUnit = TimeUnit.MINUTES,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    init {
        require(rate > 0 && bucketMaxCapacity > 0)
        require(window > 0 && timeUnit.toNanos(window) > 0)
    }

    private val capacity = bucketMaxCapacity.toDouble()
    private val tokensPerNano = rate.toDouble() / timeUnit.toNanos(window)
    private var tokens = capacity
    private var updatedAt = nanoTime()

    @Synchronized
    override fun tick(): Boolean {
        val now = nanoTime()
        tokens = minOf(capacity, tokens + (now - updatedAt) * tokensPerNano)
        updatedAt = now
        if (tokens < 1.0) return false
        tokens -= 1.0
        return true
    }
}
