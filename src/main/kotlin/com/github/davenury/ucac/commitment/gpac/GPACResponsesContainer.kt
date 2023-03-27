package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.PeersetId
import com.zopa.ktor.opentracing.span
import kotlinx.coroutines.*
import kotlinx.coroutines.future.asCompletableFuture
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GPACResponsesContainer<T>(
    private val responses: Map<PeersetId, List<Deferred<T?>>>,
    private val timeout: Duration,
) {

    private val currentState: MutableMap<PeersetId, MutableList<T>> = mutableMapOf()
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    private val shouldWait = AtomicBoolean(true)
    private val waitingForResponses = AtomicBoolean(true)
    private var overallResponses = 0
    private var success = true

    init {
        runBlocking {
            responses.forEach { (peersetId, peersetResponses) ->
                peersetResponses.forEach { deferred ->
                    deferred.asCompletableFuture().thenAccept { handleJob(peersetId, it) }
                }
            }
        }
    }

    private fun Map<*, List<*>>.flattenedSize() =
        this.values.flatten().size

    fun awaitForMessages(condition: (Map<PeersetId, List<T>>) -> Boolean): Pair<Map<PeersetId, List<T>>, Boolean> = span {
        return@span lock.withLock {
            ctx.dispatch(Dispatchers.IO) { timeout() }
            while (true) {
                logger.trace("Responses size: ${responses.flattenedSize()}, $responses, current resolved: $overallResponses")
                when {
                    !condition(currentState) && shouldWait.get() && overallResponses < responses.flattenedSize() -> {
                        logger.debug("Waiting for responses, current state: $currentState")
                        this@GPACResponsesContainer.condition.await()
                    }
                    condition(currentState) -> {
                        logger.warn("Got condition, responses: $currentState")
                        success = true
                        waitingForResponses.set(false)
                        break
                    }
                    !shouldWait.get() -> {
                        logger.warn("Waiter timeout")
                        success = false
                        waitingForResponses.set(false)
                        break
                    }
                    overallResponses >= responses.flattenedSize() -> {
                        logger.warn("Got all responses and condition wasn't satisfied")
                        success = false
                        waitingForResponses.set(false)
                        break
                    }
                }
            }
            return@withLock Pair(currentState, success)
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

    private fun handleJob(peersetId: PeersetId, value: T?) {
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

    companion object {
        private val logger = LoggerFactory.getLogger("GPACResponsesContainer")
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}
