package com.github.davenury.tests.strategies.load

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

class ConstantLoadGenerator(
    private val constantLoad: String,
) : LoadGenerator {

    private val channel: ReceiveChannel<Unit> = ticker(calculateTickerFromLoad(constantLoad), 0)
    override fun generate() {}

    override suspend fun subscribe(fn: suspend () -> Unit) {
        withContext(ctx) {
            while (true) {
                channel.receive()
                launch {
                    fn()
                }
            }
        }
    }

    override fun getName(): String = "ConstantLoadGenerator"

    private fun calculateTickerFromLoad(load: String) = (1.0 / load.toDouble() * 1000).toLong()

    companion object {
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}
