package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry


data class Ledger(
    val history: History,
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf(),
) {

    fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>) {

        val newAcceptedItems = acceptedItems - this.acceptedItems.toSet()
        this.acceptedItems.addAll(newAcceptedItems)
        newAcceptedItems.forEach { history.addEntry(it.entry) }
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
        newAcceptedItems.forEach { history.addEntry(it.entry) }
    }

    fun proposeChange(entry: HistoryEntry, iteration: Int): Int {
        val previousId = proposedItems.lastOrNull()?.id ?: acceptedItems.lastOrNull()?.id ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId,iteration, entry))
        return newId
    }

    fun getAcceptedChanges(): List<HistoryEntry> = acceptedItems.map { it.entry }.toMutableList()
    fun getProposedChanges(): List<HistoryEntry> = proposedItems.map { it.entry }.toMutableList()

    fun changeAlreadyProposed(entry: HistoryEntry): Boolean =
        (acceptedItems + proposedItems).any { it.entry == entry }

}

data class LedgerItemDto(val id: Int,val iteration: Int, val entry: String) {
    fun toLedgerItem(): LedgerItem = LedgerItem(id,iteration, HistoryEntry.deserialize(entry))
}

data class LedgerItem(val id: Int, val iteration: Int,  val entry: HistoryEntry) {
    fun toDto(): LedgerItemDto = LedgerItemDto(id,iteration, entry.serialize())
}
