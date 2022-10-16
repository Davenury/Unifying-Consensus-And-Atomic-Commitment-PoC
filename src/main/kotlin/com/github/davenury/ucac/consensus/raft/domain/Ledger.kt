package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.history.History


data class Ledger(
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf()
) {

    private var commitIndex: Int = 0
    var lastApplied = -1

    fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>): Boolean {

        val newAcceptedItems = acceptedItems - this.acceptedItems.toSet()
        this.acceptedItems.addAll(newAcceptedItems)
        val acceptedIds = acceptedItems.map { it.ledgerIndex }
        lastApplied = acceptedIds.maxOrDefault(lastApplied)


        this.proposedItems.removeAll { acceptedIds.contains(it.ledgerIndex) }
        val newProposedItems = proposedItems - this.proposedItems.toSet()
        this.proposedItems.addAll(newProposedItems)
        commitIndex = newProposedItems.maxOrDefault(commitIndex)

        return newAcceptedItems.isNotEmpty()
    }

    fun getNewAcceptedItems(ledgerIndex: Int) = acceptedItems.filter { it.ledgerIndex > ledgerIndex }
    fun getNewProposedItems(ledgerIndex: Int) = proposedItems.filter { it.ledgerIndex > ledgerIndex }

    fun acceptItems(acceptedIndexes: List<Int>) {
        val newAcceptedItems = proposedItems.filter { acceptedIndexes.contains(it.ledgerIndex) }
        acceptedItems.addAll(newAcceptedItems)
        proposedItems.removeAll(newAcceptedItems)
        lastApplied = newAcceptedItems.maxOrDefault(lastApplied)
    }

    fun proposeChange(change: Change, term: Int): Int {
        val newId = commitIndex
        proposedItems.add(LedgerItem(newId, term, change))
        commitIndex++
        return newId
    }

    fun getHistory(): History {
        // TODO why proposed??
        val h = History()
        val allChanges = (acceptedItems + proposedItems)
        println("Sizes: ${allChanges.size} /-/ ${allChanges.toSet().size}")
        acceptedItems.forEach { h.addEntry(it.change.toHistoryEntry()) }
        proposedItems.forEach { h.addEntry(it.change.toHistoryEntry()) }
        return h
    }

    fun getAcceptedChanges(): List<Change> = acceptedItems.map { it.change }.toMutableList()
    fun getProposedChanges(): List<Change> = proposedItems.map { it.change }.toMutableList()

    fun checkIfItemExist(logIndex: Int, logTerm: Int): Boolean =
        acceptedItems
            .lastOrNull()
            ?.let { it.ledgerIndex == logIndex && it.term == logTerm } ?: false

    fun removeNotAcceptedItems(logIndex: Int, logTerm: Int) {
        proposedItems.removeAll { it.ledgerIndex > logIndex || it.term > logTerm }
        acceptedItems.removeAll { it.ledgerIndex > logIndex || it.term > logTerm }
    }

    fun getLastAppliedChangeIdAndTermBeforeIndex(index: Int): Pair<Int?, Int?> =
        acceptedItems
            .sortedBy { it.ledgerIndex }
            .lastOrNull { it.ledgerIndex <= index }
            ?.let { Pair(it.ledgerIndex, it.term) }
            ?: Pair(null, null)

    fun changeAlreadyProposed(change: Change): Boolean =
        (acceptedItems + proposedItems)
            .also {
                println(
                    """Check if change (${
                        change.toHistoryEntry().getId()
                    }) exists by 
                    change: ${it.any { it.change == change }} 
                    history: ${it.any { it.change.toHistoryEntry() == change.toHistoryEntry() }} 
                    historyId: ${
                        it.any {
                            it.change.toHistoryEntry().getId() == change.toHistoryEntry().getId()
                        }
                    }"""
                )
            }
            .any { it.change.toHistoryEntry() == change.toHistoryEntry() }


    private fun List<LedgerItem>.maxOrDefault(defaultValue: Int): Int =
        this.maxOfOrNull { it.ledgerIndex } ?: defaultValue

    @JvmName("maxOrDefaultInt")
    private fun List<Int>.maxOrDefault(defaultValue: Int): Int = this.maxOfOrNull { it } ?: defaultValue

}

data class LedgerItemDto(val ledgerIndex: Int, val term: Int, val change: Change) {
    fun toLedgerItem(): LedgerItem = LedgerItem(ledgerIndex, term, change)
}

data class LedgerItem(val ledgerIndex: Int, val term: Int, val change: Change) {
    fun toDto(): LedgerItemDto = LedgerItemDto(ledgerIndex, term, change)
}
