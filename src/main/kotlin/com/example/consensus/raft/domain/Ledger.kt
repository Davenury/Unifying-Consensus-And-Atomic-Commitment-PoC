package com.example.consensus.raft.domain

import com.example.common.ChangeWithAcceptNum
import com.example.common.ChangeWithAcceptNumDto
import com.example.common.History

data class Ledger(
    var acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    var proposedItems: MutableList<LedgerItem> = mutableListOf()
) {

    fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>) {
        this.acceptedItems.addAll(acceptedItems)
        val acceptedIds = acceptedItems.map { it.id }

        this.proposedItems.removeAll { acceptedIds.contains(it.id) }
        this.proposedItems.addAll(proposedItems)
    }

    fun getNewAcceptedItems(id: Int) = acceptedItems.filter { it.id > id }
    fun getNewProposedItems(id: Int) = proposedItems.filter { it.id > id }

    fun acceptItems(acceptedIndexes: List<Int>) {
        val pair = proposedItems.partition { acceptedIndexes.contains(it.id) }
        acceptedItems.addAll(pair.first)
        proposedItems = pair.second.toMutableList()
    }

    fun proposeChange(change: ChangeWithAcceptNum): Int {
        val previousId = proposedItems.lastOrNull()?.id ?: acceptedItems.lastOrNull()?.id ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId, change))
        return newId
    }

    fun getHistory(): History =
        (acceptedItems + proposedItems).map { it.change }.toMutableList()

}

data class LedgerItemDto(val id: Int, val change: ChangeWithAcceptNumDto) {
    fun toLedgerItem(): LedgerItem = LedgerItem(id, change.toChangeWithAcceptNum())
}

data class LedgerItem(val id: Int, val change: ChangeWithAcceptNum) {
    fun toDto(): LedgerItemDto = LedgerItemDto(id, change.toDto())
}