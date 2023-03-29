package com.github.davenury.tests.strategies.load

import com.github.davenury.common.meterRegistry
import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors

class IncreasingLoadGenerator(
    private val bound: Double,
    private val increaseDelay: Duration,
    private val increaseStep: Double = 1.0,
) : LoadGenerator {

    private var channel: ReceiveChannel<Unit> = ticker(1000, 0)
    private var currentTick: Double = 1.0

    init {
        Counter.builder("current_expected_load").register(meterRegistry).increment(currentTick)
    }

    override fun generate() {
        ctx.dispatch(Dispatchers.IO) {
            runBlocking {
                while (currentTick <= bound) {
                    delay(increaseDelay.toMillis())
                    currentTick += increaseStep
                    channel = ticker((1000 / currentTick).toLong(), 0)
                    logger.info("Bumped load to $currentTick changes per second")
                    Counter.builder("current_expected_load").register(meterRegistry).increment(increaseStep)
                }
            }
        }
    }

    override suspend fun subscribe(fn: suspend () -> Unit) {
        withContext(ctx) {
            while (true) {
                channel.receive()
                fn()
            }
        }
    }

    override fun getName(): String = "IncreasingLoadGenerator"

    companion object {
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
        private val logger = LoggerFactory.getLogger("IncreasingLoadGenerator")
    }
}
