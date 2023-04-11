package com.github.davenury.tests

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.strategies.changes.CreateChangeStrategy
import com.github.davenury.tests.strategies.peersets.GetPeersStrategy
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

class Changes(
    private val peers: Map<PeersetId, List<PeerAddress>>,
    private val sender: Sender,
    private val getPeersStrategy: GetPeersStrategy,
    private val createChangeStrategy: CreateChangeStrategy,
    private val acProtocol: ACProtocol?,
    private val ownAddress: String,
) {
    private val changes = peers.mapValues { OnePeersetChanges(it.value, sender) }

    private val handledChanges: MutableMap<String, Int> = mutableMapOf()
    private val mutex = Mutex()
    private val notificationMutex = Mutex()
    private val executor = Executors.newCachedThreadPool().asCoroutineDispatcher()

    init {
        populateConsensusLeaders()
        subscribeToStructuralChanges()
    }

    private fun populateConsensusLeaders() {
        peers.map { (peersetId, _) ->
            val consensusLeaderId = GlobalScope.async {
                changes[peersetId]!!.populateConsensusLeader()
            }
            peersetId to consensusLeaderId
        }.let {
            runBlocking {
                it.map { deferred -> deferred.second }.awaitAll()
            }
        }
    }

    private fun subscribeToStructuralChanges() {
        GlobalScope.launch {
            peers.entries.forEach { (_, addresses) ->
                addresses.forEach { address ->
                    httpClient.post("http://${address.address}/v2/subscribe-to-structural-changes") {
                        contentType(ContentType.Application.Json)
                        body = SubscriberAddress(
                            address = "$ownAddress/api/v1/new-consensus-leader",
                            type = "http",
                        )
                    }
                }
            }
        }
    }

    suspend fun handleNotification(notification: Notification) = notificationMutex.withLock {
        logger.info("Handling notification: $notification")
        if (shouldStartHandlingNotification(notification)) {
            (notification.change.peersets.map { it.peersetId }).forEach { peersetId ->
                val change = getChange(notification, peersetId)
                if (notification.result.status == ChangeResult.Status.SUCCESS) {
                    val parentId = change.toHistoryEntry(peersetId).getId()
                    changes[peersetId]!!.overrideParentId(parentId)
                    logger.info("Setting new parent id for peerset $peersetId: $parentId, change was for ${(change as AddUserChange).userName}")
                } else if (notification.result.status == ChangeResult.Status.CONFLICT && notification.result.desiredParentId != null) {
                    logger.info("Change conflicted, yet we have desired parent id for peerset: ${notification.result.desiredParentId}")
                    changes[peersetId]!!.overrideParentId(notification.result.desiredParentId!!)
                }
            }
            getPeersStrategy.handleNotification(notification)
        }
    }

    private suspend fun getChange(notification: Notification, peersetId: PeersetId): Change {
        return if (acProtocol == ACProtocol.TWO_PC) {
            try {
                changes[peersetId]!!.getChange()
            } catch (e: Exception) {
                logger.error("Could not receive change from ${peers[peersetId]!!.first()}", e)
                notification.change
            }
        } else {
            notification.change
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
            val changeId = UUID.randomUUID().toString()
            val ids = getPeersStrategy.getPeersets(numberOfPeersets, changeId)
            val change = createChangeStrategy.createChange(ids, changes, changeId)

            val result = changes[ids[0]]!!.introduceChange(change)
            handleChangeResult(change, result, ids)

            change
        }
        timeoutChange(change)
    }

    private suspend fun handleChangeResult(change: Change, result: ChangeState, ids: List<PeersetId>) {
        if (result == ChangeState.ACCEPTED) {
            val historyEntriesIds = ids.map { it to change.toHistoryEntry(it).getId() }
            logger.info(
                "Introduced change $change to peersets with ids $ids with result: $result\n, entries ids will be: $historyEntriesIds"
            )
        } else {
            logger.info("Change $change was rejected, freeing peersets $ids")
            getPeersStrategy.freePeersets(ids, change.id)
        }
    }

    private fun timeoutChange(change: Change) = executor.dispatch(Dispatchers.IO) {
        runBlocking {
            delay(changeTimeout)
            notificationMutex.withLock {
                if (!handledChanges.contains(change.id)) {
                    logger.error("Change $change timed out from performance tests, freeing peersets")
                    getPeersStrategy.freePeersets(change.peersets.map { it.peersetId }, change.id)
                    handledChanges[change.id] = change.peersets.size
                }
            }
        }
    }

    fun newConsensusLeader(newConsensusLeaderId: CurrentLeaderFullInfoDto) {
        changes[newConsensusLeaderId.peersetId]!!.newConsensusLeader(newConsensusLeaderId.peerId)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("Changes")
        private const val changeTimeout: Long = 8000
    }

}

class OnePeersetChanges(
    private val peersAddresses: List<PeerAddress>,
    private val sender: Sender,
) {
    private var parentId = AtomicReference(InitialHistoryEntry.getId())
    private var consensusLeader = AtomicReference<PeerAddress?>(null)

    suspend fun introduceChange(change: Change): ChangeState {
        return try {
            sender.executeChange(
                consensusLeader.get()!!,
                change
            )
        } catch (e: Exception) {
            logger.info("Consensus leader ${consensusLeader.get()} is dead, I'm trying to get a new one")
            populateConsensusLeader()
            return introduceChange(change)
        }
    }

    suspend fun getChange(): Change {
        return try {
            httpClient.get("http://${consensusLeader.get()!!.address}/v2/last-change")
        } catch (e: Exception) {
            when (e) {
                is ClientRequestException, is ServerResponseException -> throw e
                else -> {
                    logger.info("Consensus leader ${consensusLeader.get()} is dead, I'm trying to get a new one")
                    populateConsensusLeader()
                    return getChange()
                }
            }
        }
    }

    suspend fun populateConsensusLeader() {
        consensusLeader.set(getConsensusLeader())
    }

    private suspend fun getConsensusLeader(peerAddresses: List<PeerAddress> = peersAddresses): PeerAddress {
        val address = peersAddresses.firstOrNull() ?: throw IllegalStateException("I have no more peers to ask!")
        val peerId = try {
            sender.getConsensusLeaderId(address)
        } catch (e: Exception) {
            logger.info("$address is dead, I'm trying to get consensus leader from another one")
            return getConsensusLeader(peerAddresses.filterNot { it == address })
        }
        return peersAddresses.find { it.peerId == peerId } ?: runBlocking {
            logger.info("Consensus leader is not elected yet, I'm trying to get one in 500 ms")
            delay(500)
            getConsensusLeader(peerAddresses)
        }
    }


    fun getCurrentParentId(): String = parentId.get()

    fun overrideParentId(newParentId: String) {
        parentId.set(newParentId)
    }

    fun newConsensusLeader(peerId: PeerId) {
        this.consensusLeader.set(peersAddresses.find { it.peerId == peerId }!!)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("OnePeersetChanges")
    }
}
