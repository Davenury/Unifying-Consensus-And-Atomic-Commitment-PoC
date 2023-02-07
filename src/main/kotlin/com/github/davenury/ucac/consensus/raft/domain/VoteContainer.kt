package com.github.davenury.ucac.consensus.raft.domain

class VoteContainer {
    private val map: MutableMap<String, Int> = mutableMapOf()

    fun initializeChange(id: String) = map.put(id, 0)

    fun voteForChange(id: String) = map.compute(id) { _, v -> if (v == null) 1 else v + 1 }

    fun getAcceptedChanges(filterFunction: (Int) -> Boolean) = map
        .filter { (_, value) -> filterFunction(value) }
        .map { it.key }

    fun removeChanges(indexes: List<String>) = indexes.forEach { map.remove(it) }
}
