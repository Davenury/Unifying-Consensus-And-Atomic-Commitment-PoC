package com.example.consensus.raft.domain

import com.example.common.ChangeWithAcceptNum
import com.example.common.ChangeWithAcceptNumDto
import com.example.common.History

data class Ledger(
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf()
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
        val newAcceptedItems = proposedItems.filter { acceptedIndexes.contains(it.id) }
        acceptedItems.addAll(newAcceptedItems)
        proposedItems.removeAll(newAcceptedItems)
    }

    fun proposeChange(change: ChangeWithAcceptNum): Int {
        val previousId = proposedItems.lastOrNull()?.id ?: acceptedItems.lastOrNull()?.id ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId, change))
        return newId
    }

    fun getHistory(): History =
        (acceptedItems + proposedItems).map { it.change }.toMutableList()

    fun changeAlreadyProposed(change: ChangeWithAcceptNum): Boolean =
        (acceptedItems + proposedItems).any { it.change == change }

}

data class LedgerItemDto(val id: Int, val change: ChangeWithAcceptNumDto) {
    fun toLedgerItem(): LedgerItem = LedgerItem(id, change.toChangeWithAcceptNum())
}

data class LedgerItem(val id: Int, val change: ChangeWithAcceptNum) {
    fun toDto(): LedgerItemDto = LedgerItemDto(id, change.toDto())
}