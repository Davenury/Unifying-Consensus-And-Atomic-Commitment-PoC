package com.github.davenury.tests

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Notification
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.strategies.changes.CreateChangeStrategy
import com.github.davenury.tests.strategies.peersets.GetPeersStrategy
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class Changes(
    private val peers: Map<Int, List<String>>,
    private val sender: Sender,
    private val getPeersStrategy: GetPeersStrategy,
    private val createChangeStrategy: CreateChangeStrategy
) {
    private val changes = List(peers.size) { it to OnePeersetChanges(peers[it]!!, sender) }.toMap()

    private val handledChanges: MutableList<String> = mutableListOf()
    private val mutex = Mutex()
    private val notificationMutex = Mutex()

    suspend fun handleNotification(notification: Notification) = notificationMutex.withLock {
        logger.info("Handling notification: $notification")
        if (!handledChanges.contains(notification.change.id)) {
            (notification.change.peersets.map { it.peersetId }).forEach { peersetId ->
                if (notification.result.status == ChangeResult.Status.SUCCESS) {
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

            val change = createChangeStrategy.createChange(ids, changes)

            val result = changes[ids[0]]!!.introduceChange(change)
            if (result == ChangeState.ACCEPTED) {
                logger.info("Introduced change $change to peersets with ids $ids with result: $result\n, entries ids will be: ${ids.map { it to change.toHistoryEntry(it).getId() }}")
            } else {
                logger.info("Change $change was rejected, freeing peersets $ids")
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
