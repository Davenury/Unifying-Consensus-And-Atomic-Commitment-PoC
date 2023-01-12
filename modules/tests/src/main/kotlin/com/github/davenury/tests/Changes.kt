package com.github.davenury.tests

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.strategies.GetPeersStrategy
import io.ktor.utils.io.bits.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class Changes(
    private val peers: Map<Int, List<String>>,
    private val sender: Sender,
    private val getPeersStrategy: GetPeersStrategy,
    private val ownAddress: String
) {
    private val changes = List(peers.size) { it to OnePeersetChanges(peers[it]!!, sender, ownAddress) }.toMap()

    private var counter = AtomicInteger(0)
    private val handledChanges: MutableList<String> = mutableListOf()
    private val mutex = Mutex()
    private val notificationMutex = Mutex()

    suspend fun handleNotification(notification: Notification) = notificationMutex.withLock {
        logger.info("Handling notification: $notification")
        if (!handledChanges.contains(notification.change.id)) {
            (notification.change.peersets.map { it.peersetId }).forEach { peersetId ->
                if (notification.result.status != ChangeResult.Status.CONFLICT) {
                    val parentId = notification.change.toHistoryEntry(peersetId).getId()
                    changes[peersetId]!!.overrideParentId(parentId)
                    logger.info("Setting new parent id for peerset $peersetId: $parentId, change was for ${(notification.change as AddUserChange).userName}")
                }
                getPeersStrategy.handleNotification(peersetId)
                handledChanges.add(notification.change.id)
            }
        }
    }

    suspend fun introduceChange(numberOfPeersets: Int) {
        mutex.withLock {
            val ids = getPeersStrategy.getPeersets(numberOfPeersets)
            val change = AddUserChange(
                userName = "user${counter.incrementAndGet()}",
                peersets = ids.map { ChangePeersetInfo(it, changes[it]!!.getCurrentParentId()) }
            )
            val result = changes[ids[0]]!!.introduceChange(change)
            logger.info("Introduced change $change to peersets with ids $ids with result: $result\n, entries ids will be: ${ids.map { it to change.toHistoryEntry(it).getId() }}")
            if (result == ChangeState.REJECTED) {
                getPeersStrategy.freePeersets(ids)
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("Changes")
    }

}

class OnePeersetChanges(
    private val peersAddresses: List<String>,
    private val sender: Sender,
    private val ownAddress: String
) {
    private var parentId = AtomicReference(InitialHistoryEntry.getId())

    suspend fun introduceChange(change: AddUserChange): ChangeState {
        val senderAddress = peersAddresses.first()
        return sender.executeChange(
            senderAddress,
            change.copy(
                notificationUrl = URLEncoder.encode("$ownAddress/api/v1/notification", Charset.defaultCharset())
            )
        )
    }

    fun getCurrentParentId(): String = parentId.get()

    fun overrideParentId(newParentId: String) {
        parentId.set(newParentId)
    }
}
