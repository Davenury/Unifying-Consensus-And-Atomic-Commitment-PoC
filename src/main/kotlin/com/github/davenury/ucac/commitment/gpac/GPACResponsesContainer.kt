package com.github.davenury.ucac.commitment.gpac

import kotlinx.coroutines.*
import kotlinx.coroutines.future.asCompletableFuture
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GPACResponsesContainer<T>(
    private val responses: List<List<Deferred<T?>>>,
    private val timeout: Duration,
) {

    private val currentState: MutableMap<Int, MutableList<T>> = mutableMapOf()
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val shouldWait = AtomicBoolean(true)
    private val waitingForResponses = AtomicBoolean(true)
    private var overallResponses = 0
    private var success = true

    init {
        runBlocking {
            responses.forEachIndexed { peersetId, _ ->
                responses[peersetId].forEach { deferred ->
                    deferred.asCompletableFuture().thenAccept { handleJob(peersetId, it) }
                }
            }
        }
    }
    
    private fun List<List<*>>.size() = 
        this.flatten().size

    fun awaitForMessages(condition: (List<List<T>>) -> Boolean): Pair<List<List<T>>, Boolean> {
        lock.withLock {
            ctx.dispatch(Dispatchers.IO) { timeout() }
            while (true) {
                logger.info("Responses size: ${responses.size()}, $responses, current resolved: $overallResponses")
                if (!condition(currentState.asOrderedList()) && shouldWait.get() && overallResponses < responses.size()) {
                    logger.info("Waiting for responses, current state: $currentState")
                    this.condition.await()
                } else {
                    if (condition(currentState.asOrderedList())) {
                        logger.info("Got condition, responses: ${currentState.asOrderedList()}")
                        success = true
                        waitingForResponses.set(false)
                        break
                    }
                    if (!shouldWait.get()) {
                        logger.info("Waiter timeout")
                        success = false
                        waitingForResponses.set(false)
                        break
                    }
                    if (overallResponses >= responses.size()) {
                        logger.info("Got all responses and condition wasn't satisfied")
                        success = false
                        waitingForResponses.set(false)
                        break
                    }
                }
            }
            return Pair(currentState.asOrderedList(), success)
        }
    }

    private fun timeout() {
        runBlocking {
            delay(timeout.toMillis())
            shouldWait.set(false)
            lock.withLock {
                condition.signalAll()
            }
        }
    }

    private fun handleJob(peersetId: Int, value: T?) {
        lock.withLock {
            if (waitingForResponses.get()) {
                if (value != null) {
                    currentState[peersetId] = currentState.getOrDefault(peersetId, mutableListOf()).also { it.add(value) }
                }
                overallResponses++
                condition.signalAll()
            }
        }
    }

    private fun MutableMap<Int, MutableList<T>>.asOrderedList() =
        this.entries.sortedBy { it.key }.toList().map { (_, list) -> list }

    companion object {
        private val logger = LoggerFactory.getLogger("GPACResponsesContainer")
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }

}
