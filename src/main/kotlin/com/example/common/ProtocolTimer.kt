package com.example.common

import kotlinx.coroutines.*
import java.time.Duration
import java.util.*
import kotlin.math.absoluteValue

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
    fun setDelay(delay: Duration)
}

class ProtocolTimerImpl(
    private var delay: Duration,
    private val backoffBound: Duration
) : ProtocolTimer {

    private var task: Job? = null

    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        task = GlobalScope.launch(Dispatchers.IO) {
            delay(delay.toMillis() + randomGenerator.nextLong() % backoffBound.toMillis())
            action()
        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

    override fun setDelay(delay: Duration) {
        this.delay = delay
    }
}
