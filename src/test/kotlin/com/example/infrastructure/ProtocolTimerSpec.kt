package com.example.infrastructure

import com.example.utils.atLeast
import com.example.utils.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class ProtocolTimerSpec {

    @Test
    fun `should execute in timeout`(): Unit = runBlocking {
        val delayInSeconds = 2
        val subject = ProtocolTimerImpl(delayInSeconds, 1)

        val list = mutableListOf<Int>()

        subject.startCounting { list.add(1) }

        eventually(3) {
            expectThat(list.size).isEqualTo(1)
        }
    }

    @Test
    fun `should not execute before timeout`(): Unit = runBlocking {
        val delayInSeconds = 2
        val subject = ProtocolTimerImpl(delayInSeconds, 1)

        val list = mutableListOf<Int>()

        subject.startCounting { list.add(1) }

        // atLeast will throw exception if assertion will be fulfilled before 1 second timeout
        atLeast(1) { expectThat(list.size).isEqualTo(1) }
    }

    @Test
    fun `should be able to cancel job`(): Unit = runBlocking {
        val delayInSeconds = 3
        val subject = ProtocolTimerImpl(delayInSeconds, 1)

        val list = mutableListOf<Int>()

        subject.startCounting { list.add(1) }

        subject.cancelCounting()

        // wait five second to see if we actually cancelled this job
        delay(5)

        expectThat(list.size).isEqualTo(0)
    }

}