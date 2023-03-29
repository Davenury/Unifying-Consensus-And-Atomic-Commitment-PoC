package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.Notification
import com.github.davenury.common.PeersetId
import com.github.davenury.tests.Metrics
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RandomPeersWithDelayOnConflictStrategy(
    private val peersets: List<PeersetId>,
    private val lock: Lock = ReentrantLock(),
    private val condition: Condition = lock.newCondition()
) : GetPeersStrategy {

    private val changeToLockedPeersets: ConcurrentHashMap<String, List<PeersetId>> = ConcurrentHashMap()

    override suspend fun getPeersets(numberOfPeersets: Int, changeId: String): List<PeersetId> =
        lock.withLock {
            lateinit var ids: List<PeersetId>
            var metricBumped = false
            while (true) {
                ids = peersets.filter { it !in changeToLockedPeersets.values.flatten() }.shuffled().take(numberOfPeersets)
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
            changeToLockedPeersets[changeId] = ids
            return@withLock ids
        }

    override suspend fun freePeersets(peersetsId: List<PeersetId>, changeId: String) {
        lock.withLock {
            changeToLockedPeersets[changeId] = listOf()
            condition.signalAll()
        }
    }

    override suspend fun handleNotification(notification: Notification) {
        lock.withLock {
            changeToLockedPeersets[notification.change.id] = listOf()
            condition.signalAll()
        }
    }
}
