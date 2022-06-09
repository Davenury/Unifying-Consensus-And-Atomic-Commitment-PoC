package com.example.infrastructure

import com.example.domain.ProtocolTimer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class ProtocolTimerImpl(
    private val delay: Int
): ProtocolTimer {

    private var task: Job? = null

    override fun startCounting(action: suspend () -> Unit) {
        task = GlobalScope.launch {
            delay((delay * 1000).toLong())
            action()
        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }
}