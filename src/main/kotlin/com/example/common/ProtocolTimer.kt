package com.example.common

import kotlinx.coroutines.*
import java.util.*

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
}

class ProtocolTimerImpl(
    private val delay: Int,
    private val backoffBound: Long
) : ProtocolTimer {

    private var task: Job? = null
    private val millisInSecond = 1000

    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        task = GlobalScope.launch(Dispatchers.IO) {
            delay((delay + randomGenerator.nextLong() % backoffBound) * millisInSecond)
            action()
        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }
}