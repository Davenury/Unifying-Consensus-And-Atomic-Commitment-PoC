package com.github.davenury.tests.strategies.load

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import java.time.Duration
import java.util.concurrent.Executors

class IncreasingLoadGenerator(
    private val bound: Double,
    private val increaseDelay: Duration,
    private val increaseStep: Double = 1.0,
) : LoadGenerator {

    private var channel: ReceiveChannel<Unit> = ticker(1000, 0)
    private var currentTick = 1.0

    override fun generate() {
        ctx.dispatch(Dispatchers.IO) {
            runBlocking {
                while (currentTick <= bound) {
                    delay(increaseDelay.toMillis())
                    val newTick = currentTick + increaseStep
                    channel = ticker((1000 / newTick).toLong(), 0)
                    currentTick = newTick
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
    }
}