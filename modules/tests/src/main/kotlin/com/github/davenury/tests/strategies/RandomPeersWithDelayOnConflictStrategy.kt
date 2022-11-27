package com.github.davenury.tests.strategies

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RandomPeersWithDelayOnConflictStrategy(
    private val peersetsRange: IntRange,
    private val lock: Lock = ReentrantLock(),
    private val condition: Condition = lock.newCondition()
) : GetPeersStrategy {
    private val lockedPeersets: ConcurrentHashMap<Int, Boolean> =
        ConcurrentHashMap(peersetsRange.zip(List(peersetsRange.count()) { false }).toMap())

    override suspend fun getPeersets(numberOfPeersets: Int): List<Int> =
        lock.withLock {
            lateinit var ids: List<Int>
            while (true) {
                ids = peersetsRange.filter { lockedPeersets[it] == false }.shuffled().take(numberOfPeersets)
                if (ids.size < numberOfPeersets) {
                    condition.await()
                } else {
                    break
                }
            }
            ids.forEach { lockedPeersets[it] = true }
            return@withLock ids
        }

    override suspend fun freePeersets(peersetsId: List<Int>) {
        lock.withLock {
            peersetsId.forEach {
                lockedPeersets[it] = false
            }
            condition.signalAll()
        }
    }

    override suspend fun handleNotification(peersetId: Int) {
        lock.withLock {
            lockedPeersets[peersetId] = false
            condition.signalAll()
        }
    }
}