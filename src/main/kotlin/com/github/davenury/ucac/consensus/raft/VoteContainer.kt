package com.github.davenury.ucac.consensus.raft

import com.github.davenury.ucac.common.PeerAddress

class VoteContainer {
    private val map: MutableMap<String, List<PeerAddress>> = mutableMapOf()

    fun getVotes(id: String) = map[id]

    fun initializeChange(id: String) = map.put(id, listOf())

    fun voteForChange(id: String, address: PeerAddress) =
        map.compute(id) { _, v -> if (v == null) listOf(address) else (v + listOf(address)).distinct() }

    fun getAcceptedChanges(filterFunction: (Int) -> Boolean) = map
        .filter { (_, value) -> filterFunction(value.size) }
        .map { it.key }

    fun removeChanges(indexes: List<String>) = indexes.forEach { map.remove(it) }
}
