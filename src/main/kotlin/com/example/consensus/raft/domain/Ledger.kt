package com.example.consensus.raft.domain

import com.example.common.ChangeWithAcceptNum
import com.example.common.ChangeWithAcceptNumDto
import com.example.common.History

data class Ledger(
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf()
) {

    fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>) {

        val newAcceptedItems = acceptedItems.filterNot { this.acceptedItems.contains(it) }
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

    fun proposeChange(change: ChangeWithAcceptNum, iteration: Int): Int {
        val previousId = proposedItems.lastOrNull()?.id ?: acceptedItems.lastOrNull()?.id ?: -1
        val newId = previousId + 1
        proposedItems.add(LedgerItem(newId,iteration, change))
        return newId
    }

    fun getHistory(): History =
        (acceptedItems + proposedItems).map { it.change }.toMutableList()

    fun getAcceptedChanges(): History = acceptedItems.map { it.change }.toMutableList()
    fun getProposedChanges(): History = proposedItems.map { it.change }.toMutableList()

    fun changeAlreadyProposed(change: ChangeWithAcceptNum): Boolean =
        (acceptedItems + proposedItems).any { it.change == change }

}

data class LedgerItemDto(val id: Int,val iteration: Int, val change: ChangeWithAcceptNumDto) {
    fun toLedgerItem(): LedgerItem = LedgerItem(id,iteration, change.toChangeWithAcceptNum())
}

data class LedgerItem(val id: Int, val iteration: Int,  val change: ChangeWithAcceptNum) {
    fun toDto(): LedgerItemDto = LedgerItemDto(id,iteration, change.toDto())
}