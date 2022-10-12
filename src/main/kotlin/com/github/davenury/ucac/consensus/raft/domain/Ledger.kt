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
        val acceptedIds = acceptedItems.map { it.id }


        this.proposedItems.removeAll { acceptedIds.contains(it.id) }
        this.proposedItems.addAll(proposedItems)

    }

    fun getNewAcceptedItems(id: Int) = acceptedItems.filter { it.id > id }
    fun getNewProposedItems(id: Int) = proposedItems.filter { it.id > id }
    fun getLastAcceptedItemId() = acceptedItems.lastOrNull()?.id ?: -1

    fun acceptItems(acceptedIndexes: List<Int>) {
        val newAcceptedItems = proposedItems.filter { acceptedIndexes.contains(it.id) }
        acceptedItems.addAll(newAcceptedItems)
        proposedItems.removeAll(newAcceptedItems)
    }

    fun proposeChange(change: Change, iteration: Int): Int {
        val previousId = proposedItems.lastOrNull()?.id ?: acceptedItems.lastOrNull()?.id ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId, iteration, change))
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

data class LedgerItemDto(val id: Int, val iteration: Int, val change: Change) {
    fun toLedgerItem(): LedgerItem = LedgerItem(id, iteration, change)
}

data class LedgerItem(val id: Int, val iteration: Int, val change: Change) {
    fun toDto(): LedgerItemDto = LedgerItemDto(id, iteration, change)
}
