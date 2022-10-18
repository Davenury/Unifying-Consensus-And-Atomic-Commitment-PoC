package com.github.davenury.ucac.common

import kotlinx.coroutines.*
import java.time.Duration
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

interface ProtocolTimer {
    suspend fun startCounting(iteration: Int = 0, action: suspend () -> Unit)
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

    override suspend fun startCounting(iteration: Int, action: suspend () -> Unit) {
        cancelCounting()
        with(CoroutineScope(ctx)) {

            task = launch {

                val exponent = 1.5.pow(iteration)

                val backoff = (
                        if (backoffBound.isZero) 0
                        else randomGenerator.nextLong().absoluteValue % (backoffBound.toMillis() * exponent).toLong()
                        )
                    .let { Duration.ofMillis(it) }
                val timeout = delay.plus(backoff)
                if (iteration != 0) println("Sleeping for: $timeout /-/ $backoff /-/ $exponent /-/ $iteration")
                delay(timeout.toMillis())
                action()
            }

        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

}
