package com.github.davenury.tests

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.strategies.GetPeersStrategy
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/*
* 1. Config for atomic commitment and consensus - which one to use
* 2. Strategies for sending changes to peersets - what if there's no free peerset?
* 3. Tests should have metrics too - e.g. how many requests per second are we sending
* */

class Changes(
    private val peers: Map<Int, List<String>>,
    private val sender: Sender,
    private val getPeersStrategy: GetPeersStrategy
) {
    private val changes = List(peers.size) { it to OnePeersetChanges(peers[it]!!, sender) }.toMap()

    private var counter = AtomicInteger(0)

    suspend fun handleNotification(notification: Notification) {
        if (notification.result.status == ChangeResult.Status.SUCCESS) {
            notification.change.peers.forEach {
                val peersetId = findPeer(it)
                changes[peersetId]!!.overrideParentId(notification.change.toHistoryEntry().getId())
                getPeersStrategy.handleNotification(peersetId)
            }
        }
    }

    suspend fun introduceChange(numberOfPeersets: Int): Change {
        val ids = getPeersStrategy.getPeersets(numberOfPeersets)
        return changes[ids[0]]!!.introduceChange(counter.getAndIncrement(), *ids.drop(1).map { peers[it]!!.first() }.toTypedArray())
    }

    private fun findPeer(address: String): Int =
        peers.filter { (_, peersAddresses) -> address in peersAddresses }
            .entries
            .firstOrNull()
            ?.key ?: throw AssertionError("Peer $address was not found in map of peers")

}

class OnePeersetChanges(
    private val peersAddresses: List<String>,
    private val sender: Sender,
) {
    private var parentId = AtomicReference(InitialHistoryEntry.getId())

    suspend fun introduceChange(counter: Int, vararg otherPeers: String): Change {
        val senderAddress = peersAddresses.asSequence().shuffled().find { true }!!
        val change = AddUserChange(getCurrentParentId(), "userName${counter}", otherPeers.asList())
        sender.executeChange(senderAddress, change)
        return change
    }

    fun getCurrentParentId(): String = parentId.get()

    fun overrideParentId(newParentId: String) {
        parentId.set(newParentId)
    }
}
