package com.github.davenury.tests.strategies.load

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.Executors

class BoundLoadGenerator(
    private val overallNumberOfRequests: Int,
    timeOfSimulation: Duration,
) : LoadGenerator {

    private val sendRequestBreak = timeOfSimulation.dividedBy(overallNumberOfRequests.toLong())
    private val channel: ReceiveChannel<Unit> = ticker(sendRequestBreak.toMillis(), 0)

    override fun generate() {}

    override suspend fun subscribe(fn: suspend () -> Unit) {
        withContext(ctx) {
            for (i in (1..overallNumberOfRequests)) {
                channel.receive()
                launch {
                    fn()
                }
            }
        }
    }

    override fun getName(): String = "BoundLoadGenerator"

    companion object {
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}