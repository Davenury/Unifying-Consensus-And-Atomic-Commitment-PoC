package com.github.davenury.ucac.common

import kotlinx.coroutines.*
import java.time.Duration
import java.util.*
import kotlin.coroutines.coroutineContext
import kotlin.math.absoluteValue

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
    fun setDelay(delay: Duration)
}

class ProtocolTimerImpl(
    private var delay: Duration,
    private val backoffBound: Duration,
    private val ctx: ExecutorCoroutineDispatcher
) : ProtocolTimer {

    private var task: Job? = null

    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        with(CoroutineScope(ctx)) {
            task = launch {
                val timeout = delay.toMillis() + randomGenerator.nextLong().absoluteValue % backoffBound.toMillis()
                delay(timeout)
                action()
            }

        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

    override fun setDelay(delay: Duration) {
        this.delay = delay
    }
}
