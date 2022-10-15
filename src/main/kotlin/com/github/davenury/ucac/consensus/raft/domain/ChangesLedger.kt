package com.github.davenury.ucac.consensus.raft.domain

class ChangesLedger {
    private val map: MutableMap<Int, Int> = mutableMapOf()

    public fun initializeKey(id: Int) = map.put(id, 0)

    public fun incrementKey(id: Int) = map.compute(id) { _, v -> if (v == null) 1 else v + 1 }

    public fun getAcceptedIndexes(filterFunction: (Int) -> Boolean) = map
        .filter { (_, value) -> filterFunction(value) }
        .map { it.key }

    public fun removeIndexes(indexes: List<Int>) = indexes.forEach { map.remove(it) }
}