package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.history.History


data class Ledger(
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf()
) {

    fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>) {

        val newAcceptedItems = acceptedItems - this.acceptedItems.toSet()
        this.acceptedItems.addAll(newAcceptedItems)
        val acceptedIds = acceptedItems.map { it.ledgerIndex }


        this.proposedItems.removeAll { acceptedIds.contains(it.ledgerIndex) }
        this.proposedItems.addAll(proposedItems)

    }

    fun getNewAcceptedItems(ledgerIndex: Int) = acceptedItems.filter { it.ledgerIndex > ledgerIndex }
    fun getNewProposedItems(ledgerIndex: Int) = proposedItems.filter { it.ledgerIndex > ledgerIndex }
    fun getLastAcceptedItemId() = acceptedItems.lastOrNull()?.ledgerIndex ?: -1

    fun acceptItems(acceptedIndexes: List<Int>) {
        val newAcceptedItems = proposedItems.filter { acceptedIndexes.contains(it.ledgerIndex) }
        acceptedItems.addAll(newAcceptedItems)
        proposedItems.removeAll(newAcceptedItems)
    }

    fun proposeChange(change: Change, term: Int): Int {
        val previousId = proposedItems.lastOrNull()?.ledgerIndex ?: acceptedItems.lastOrNull()?.ledgerIndex ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId, term, change))
        return newId
    }

    fun getHistory(): History {
        // TODO why proposed??
        val h = History()
        acceptedItems.forEach { h.addEntry(it.change.toHistoryEntry()) }
        proposedItems.forEach { h.addEntry(it.change.toHistoryEntry()) }
        return h
    }

    fun getAcceptedChanges(): List<Change> = acceptedItems.map { it.change }.toMutableList()
    fun getProposedChanges(): List<Change> = proposedItems.map { it.change }.toMutableList()

    fun changeAlreadyProposed(change: Change): Boolean =
        (acceptedItems + proposedItems).any { it.change == change }

}

data class LedgerItemDto(val ledgerIndex: Int, val term: Int, val change: Change) {
    fun toLedgerItem(): LedgerItem = LedgerItem(ledgerIndex,term, change)
}

data class LedgerItem(val ledgerIndex: Int, val term: Int, val change: Change) {
    fun toDto(): LedgerItemDto = LedgerItemDto(ledgerIndex,term, change)
}
