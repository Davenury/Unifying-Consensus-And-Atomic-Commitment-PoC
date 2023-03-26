package com.github.davenury.tests

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.strategies.changes.CreateChangeStrategy
import com.github.davenury.tests.strategies.peersets.GetPeersStrategy
import io.ktor.client.request.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

class Changes(
    private val peers: Map<PeersetId, List<PeerAddress>>,
    private val sender: Sender,
    private val getPeersStrategy: GetPeersStrategy,
    private val createChangeStrategy: CreateChangeStrategy,
    private val acProtocol: ACProtocol?
) {
    private val changes = peers.mapValues { OnePeersetChanges(it.value, sender) }

    private val handledChanges: MutableMap<String, Int> = mutableMapOf()
    private val mutex = Mutex()
    private val notificationMutex = Mutex()
    private val executor = Executors.newCachedThreadPool().asCoroutineDispatcher()

    suspend fun handleNotification(notification: Notification) = notificationMutex.withLock {
        logger.info("Handling notification: $notification")
        if (shouldStartHandlingNotification(notification)) {
            (notification.change.peersets.map { it.peersetId }).forEach { peersetId ->
                val change = if (acProtocol == ACProtocol.TWO_PC) {
                    try {
                        httpClient.get("http://${peers[peersetId]!!.first()}/v2/last-change")
                    } catch (e: Exception) {
                        logger.error("Could not receive change from ${peers[peersetId]!!.first()}", e)
                        notification.change
                    }
                } else {
                    notification.change
                }
                if (notification.result.status == ChangeResult.Status.SUCCESS) {
                    val parentId = change.toHistoryEntry(peersetId).getId()
                    changes[peersetId]!!.overrideParentId(parentId)
                    logger.info("Setting new parent id for peerset $peersetId: $parentId, change was for ${(change as AddUserChange).userName}")
                }
            }
            getPeersStrategy.handleNotification(notification)
        }
    }

    private fun shouldStartHandlingNotification(notification: Notification): Boolean =
        when {
            acProtocol != ACProtocol.TWO_PC && handledChanges.contains(notification.change.id) -> false
            acProtocol != ACProtocol.TWO_PC -> kotlin.run {
                handledChanges[notification.change.id] = 1
                return@run true
            }
            notification.result.status != ChangeResult.Status.SUCCESS -> kotlin.run {
                handledChanges[notification.change.id] = 1
                return@run true
            }
            else -> kotlin.run {
                handledChanges[notification.change.id] = handledChanges.getOrDefault(notification.change.id, 0) + 1
                return@run handledChanges[notification.change.id]!! >= notification.change.peersets.size
            }
        }

    suspend fun introduceChange(numberOfPeersets: Int) {
        val change = mutex.withLock {
            val ids = getPeersStrategy.getPeersets(numberOfPeersets)
            val change = createChangeStrategy.createChange(ids, changes)
            getPeersStrategy.setCurrentChange(change.id)

            val result = changes[ids[0]]!!.introduceChange(change)

            if (result == ChangeState.ACCEPTED) {
                val historyEntriesIds = ids.map { it to change.toHistoryEntry(it).getId() }
                logger.info(
                    "Introduced change $change to peersets with ids $ids with result: $result\n, entries ids will be: $historyEntriesIds"
                )
            } else {
                logger.info("Change $change was rejected, freeing peersets $ids")
                getPeersStrategy.freePeersets(ids)
            }
            change
        }
        executor.dispatch(Dispatchers.IO) {
            runBlocking {
                delay(changeTimeout)
                notificationMutex.withLock {
                    if (!handledChanges.contains(change.id)) {
                        logger.error("Change $change timed out from performance tests, freeing peersets")
                        getPeersStrategy.freePeersets(change.peersets.map { it.peersetId })
                        handledChanges[change.id] = change.peersets.size
                    }
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("Changes")
        private const val changeTimeout: Long = 8000
    }

}

class OnePeersetChanges(
    private val peersAddresses: List<PeerAddress>,
    private val sender: Sender
) {
    private var parentId = AtomicReference(InitialHistoryEntry.getId())

    suspend fun introduceChange(change: Change): ChangeState {
        val senderAddress = peersAddresses.first()
        return sender.executeChange(
            senderAddress,
            change
        )
    }

    fun getCurrentParentId(): String = parentId.get()

    fun overrideParentId(newParentId: String) {
        parentId.set(newParentId)
    }
}