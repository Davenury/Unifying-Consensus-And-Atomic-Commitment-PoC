package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.ucac.common.ProtocolTimerImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.internal.notify
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timer
import kotlin.concurrent.withLock

class GPACResponsesContainer {

    private val electWaiter = Waiter<ElectedYou>()
    private val agreeWaiter = Waiter<Agreed>()
    private val applyWaiter = Waiter<Applied>()

    fun waitForElectResponses(condition: (List<List<ElectedYou>>) -> Boolean): List<List<ElectedYou>> =
        electWaiter.waitForResponses(condition)

    fun waitForAgreeResponses(condition: (List<List<Agreed>>) -> Boolean): List<List<Agreed>> =
        agreeWaiter.waitForResponses(condition)

    fun waitForApplyResponses(condition: (List<List<Applied>>) -> Boolean) =
        applyWaiter.waitForResponses(condition)

    fun addElectResponse(response: ElectedYou) {
        electWaiter.addResponse(response)
    }

    fun addAgreeResponse(response: Agreed) {
        agreeWaiter.addResponse(response)
    }

    fun addApplyResponse(response: Applied) {
        applyWaiter.addResponse(response)
    }

    private class Waiter<T: GpacResponse> {
        private val ctx = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        private val lock = ReentrantLock()
        private val lockCondition = lock.newCondition()
        private val responseContainer: MutableMap<Int, MutableList<T>> = mutableMapOf()
        private var shouldWait = true

        fun addResponse(response: T) {
            lock.withLock {
                val peersetId = response.sender.peersetId
                responseContainer[peersetId]?.add(response) ?: kotlin.run {
                    responseContainer[peersetId] = mutableListOf(response)
                }
                logger.info("Got response from peerset: $peersetId: $response")
                lockCondition.signalAll()
            }
        }

        fun waitForResponses(condition: (List<List<T>>) -> Boolean): List<List<T>> {
            lock.withLock {
                ctx.dispatch(Dispatchers.IO) { timeout() }
                while (true) {
                    if (!condition(responseContainer.asOrderedList()) && shouldWait) {
                        logger.info("Waiting for responses - current responses: $responseContainer")
                        lockCondition.await()
                    } else {
                        if (!shouldWait) {
                            logger.info("Waiter timeout")
                        } else {
                            logger.info("Condition is ok: $responseContainer")
                        }
                        break
                    }
                }
            }
            return responseContainer.asOrderedList()
        }

        private fun MutableMap<Int, MutableList<T>>.asOrderedList() = this.entries.sortedBy { it.key }.toList().map { (_, list) -> list }

        private fun timeout() {
            runBlocking {
                // TODO - move to config if this solution is better
                delay(2000)
                shouldWait = false
                lock.withLock {
                    lockCondition.signalAll()
                }
            }
        }

        companion object {
            private val logger = LoggerFactory.getLogger("Waiter")
        }
    }

}