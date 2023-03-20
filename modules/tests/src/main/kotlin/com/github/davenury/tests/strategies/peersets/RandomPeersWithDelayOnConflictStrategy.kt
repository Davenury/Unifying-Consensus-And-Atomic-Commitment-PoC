package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.PeersetId
import com.github.davenury.common.Notification
import com.github.davenury.tests.Metrics
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RandomPeersWithDelayOnConflictStrategy(
    private val peersets: List<PeersetId>,
    private val lock: Lock = ReentrantLock(),
    private val condition: Condition = lock.newCondition()
) : GetPeersStrategy {
    private val lockedPeersets: ConcurrentHashMap<PeersetId, Boolean> =
        ConcurrentHashMap(peersets.zip(List(peersets.count()) { false }).toMap())
    private val currentlyBlockedOnChange = AtomicReference<String>()

    override suspend fun getPeersets(numberOfPeersets: Int): List<PeersetId> =
        lock.withLock {
            lateinit var ids: List<PeersetId>
            var metricBumped = false
            while (true) {
                ids = peersets.filter { lockedPeersets[it] == false }.shuffled().take(numberOfPeersets)
                if (ids.size < numberOfPeersets) {
                    if (!metricBumped) {
                        Metrics.bumpDelayInSendingChange()
                        metricBumped = true
                    }
                    condition.await()
                } else {
                    break
                }
            }
            ids.forEach { lockedPeersets[it] = true }
            return@withLock ids
        }

    override suspend fun freePeersets(peersetsId: List<PeersetId>) {
        lock.withLock {
            peersetsId.forEach {
                lockedPeersets[it] = false
            }
            condition.signalAll()
        }
    }

    override suspend fun handleNotification(notification: Notification) {
        lock.withLock {
            if (currentlyBlockedOnChange.get() == notification.change.id) {
                notification.change.peersets.forEach {
                    lockedPeersets[it.peersetId] = false
                }
                condition.signalAll()
            }
        }
    }

    override fun setCurrentChange(changeId: String) {
        this.currentlyBlockedOnChange.set(changeId)
    }
}