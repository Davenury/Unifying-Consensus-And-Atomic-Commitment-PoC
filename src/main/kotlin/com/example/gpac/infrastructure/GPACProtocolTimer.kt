package com.example.gpac.infrastructure

import com.example.common.ProtocolTimer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.*

class GPACProtocolTimer(
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
        task = GlobalScope.launch {
            delay((delay + randomGenerator.nextLong() % backoffBound) * millisInSecond)
            action()
        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }
}