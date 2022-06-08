package com.example.infrastructure

import com.example.domain.ProtocolTimer
import java.util.*

class ProtocolTimerImpl(
    private val delay: Int
): ProtocolTimer {

    private val timer = Timer()

    override fun startCounting(fn: () -> Unit) {
        this.timer.schedule(object: TimerTask() {
            override fun run() {
                fn()
            }
        }, (delay * 1000).toLong())
    }

    override fun cancelCounting() {
        this.timer.cancel()
    }
}