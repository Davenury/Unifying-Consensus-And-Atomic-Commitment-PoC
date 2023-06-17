package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.PeerAddress
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class VoteContainer {
    private val map: MutableMap<String, List<PeerAddress>> = mutableMapOf()
    private val mutex = Mutex()

    suspend fun getVotes(id: String) = mutex.withLock { map[id] }

    suspend fun initializeChange(id: String) = mutex.withLock { map.put(id, listOf()) }

    suspend fun voteForChange(id: String, address: PeerAddress) = mutex.withLock {
        map.compute(id) { _, v -> if (v == null) listOf(address) else (v + listOf(address)).distinct() }
    }

    suspend fun getAcceptedChanges(filterFunction: (Int) -> Boolean) = mutex.withLock {
        map
            .filter { (_, value) -> filterFunction(value.size) }
            .map { it.key }
    }

    suspend fun removeChanges(indexes: List<String>) = mutex.withLock {
        indexes.forEach { map.remove(it) }
    }
}
