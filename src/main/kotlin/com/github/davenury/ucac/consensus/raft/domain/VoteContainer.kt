package com.github.davenury.ucac.consensus.raft.domain

class VoteContainer {
    private val map: MutableMap<Int, Int> = mutableMapOf()

    fun initializeChange(id: Int) = map.put(id, 0)

    fun voteForChange(id: Int) = map.compute(id) { _, v -> if (v == null) 1 else v + 1 }

    fun getAcceptedChanges(filterFunction: (Int) -> Boolean) = map
        .filter { (_, value) -> filterFunction(value) }
        .map { it.key }

    fun removeChanges(indexes: List<Int>) = indexes.forEach { map.remove(it) }
}
