package com.github.davenury.ucac.common

import kotlinx.coroutines.*
import java.time.Duration
import java.util.*
import kotlin.coroutines.coroutineContext
import kotlin.math.absoluteValue

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
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
                val backoff =
                    if (backoffBound.isZero) 0 else randomGenerator.nextLong().absoluteValue % backoffBound.toMillis()
                val timeout = delay.toMillis() + backoff
                delay(timeout)
                action()
            }

        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

}
