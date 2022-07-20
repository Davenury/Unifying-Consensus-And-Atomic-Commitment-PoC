package com.example.common

import java.util.*
import kotlinx.coroutines.*
import kotlin.math.absoluteValue

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
    fun setDelay(delay: Int)
    fun setBackOffBound(backoffBound: Long)
}

class ProtocolTimerImpl(private var delay: Int, private var backoffBound: Long) : ProtocolTimer {

    private var task: Job? = null


    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        task =
            GlobalScope.launch(Dispatchers.IO) {
                val milliseconds = delay + randomGenerator.nextLong().absoluteValue % backoffBound
                delay(milliseconds)
                action()
            }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

    override fun setDelay(delay: Int) {
        this.delay = delay
    }

    override fun setBackOffBound(backoffBound: Long) {
        this.backoffBound = backoffBound
    }
}
