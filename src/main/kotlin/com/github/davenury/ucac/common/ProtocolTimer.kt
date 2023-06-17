package com.github.davenury.ucac.common

import com.zopa.ktor.opentracing.tracingContext
import kotlinx.coroutines.*
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

interface ProtocolTimer {
    suspend fun startCounting(iteration: Int = 0, action: suspend () -> Unit)
    fun cancelCounting()

    fun isTaskFinished(): Boolean
}

class ProtocolTimerImpl(
    private var delay: Duration,
    private val backoffBound: Duration,
    private val ctx: ExecutorCoroutineDispatcher
) : ProtocolTimer {

    private var task: Job? = null

    companion object {
        private val randomGenerator = Random()
        private val logger = LoggerFactory.getLogger("protocolTimer")
    }

    override suspend fun startCounting(iteration: Int, action: suspend () -> Unit) {
        cancelCounting()
        with(CoroutineScope(ctx) + tracingContext()) {
            task = launch(MDCContext() + tracingContext()) {
                val exponent = 1.5.pow(iteration)

                val backoff = (
                        if (backoffBound.isZero) 0
                        else randomGenerator.nextLong().absoluteValue % (backoffBound.toMillis() * exponent).toLong()
                        )
                    .let { Duration.ofMillis(it) }
                val timeout = delay.plus(backoff)
                delay(timeout.toMillis())
                action()
            }
        }
    }

    override fun cancelCounting() {
        try {
            this.task?.cancel()
        } catch (e: CancellationException) {
            logger.error("Cancellation exception occurred", e)
        }
    }

    override fun isTaskFinished(): Boolean = task == null || task?.isCompleted ==true || task?.isCancelled == true


}
