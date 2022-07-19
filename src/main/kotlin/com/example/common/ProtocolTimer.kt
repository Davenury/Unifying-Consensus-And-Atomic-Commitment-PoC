package com.example.common

import java.util.*
import kotlinx.coroutines.*

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
    fun setDelay(delay: Int)
}

class ProtocolTimerImpl(private var delay: Int, private val backoffBound: Long) : ProtocolTimer {

    private var task: Job? = null


    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        task =
            GlobalScope.launch(Dispatchers.IO) {
                delay(delay + randomGenerator.nextLong() % backoffBound)
                action()
            }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

    override fun setDelay(delay: Int) {
        this.delay = delay
    }
}
