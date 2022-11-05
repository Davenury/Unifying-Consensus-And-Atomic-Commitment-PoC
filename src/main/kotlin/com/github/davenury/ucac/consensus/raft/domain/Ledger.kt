package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock


data class Ledger(
    private val history: History,
    val acceptedItems: MutableList<LedgerItem> = mutableListOf(),
    val proposedItems: MutableList<LedgerItem> = mutableListOf(),
    private val mutex: Mutex = Mutex(),
) {

    var commitIndex: Int = 0
    var lastApplied = -1

    suspend fun updateLedger(acceptedItems: List<LedgerItem>, proposedItems: List<LedgerItem>): LedgerUpdateResult {
        mutex.withLock {
            val newAcceptedItems = acceptedItems - this.acceptedItems.toSet()
            this.acceptedItems.addAll(newAcceptedItems)
            val acceptedIds = acceptedItems.map { it.ledgerIndex }
            lastApplied = acceptedIds.maxOrDefault(lastApplied)

            this.proposedItems.removeAll { acceptedIds.contains(it.ledgerIndex) }
            val newProposedItems = proposedItems - this.proposedItems.toSet()
            this.proposedItems.addAll(newProposedItems)
            commitIndex = newProposedItems.maxOrDefault(commitIndex)

            newAcceptedItems.forEach { history.addEntry(it.change.toHistoryEntry()) }
            return LedgerUpdateResult(
                anyAcceptedChange = newAcceptedItems.isNotEmpty(),
                anyProposedChange = newProposedItems.isNotEmpty()
            )
        }
    }

    suspend fun getNewAcceptedItems(ledgerIndex: Int) =
        mutex.withLock {
            acceptedItems.filter { it.ledgerIndex > ledgerIndex }
        }

    suspend fun getNewProposedItems(ledgerIndex: Int) =
        mutex.withLock {
            proposedItems.filter { it.ledgerIndex > ledgerIndex }
        }

    suspend fun acceptItems(acceptedIndexes: List<Int>) =
        mutex.withLock {
            val newAcceptedItems = proposedItems.filter { acceptedIndexes.contains(it.ledgerIndex) }
            acceptedItems.addAll(newAcceptedItems)
            newAcceptedItems.forEach { history.addEntry(it.change.toHistoryEntry()) }
            proposedItems.removeAll(newAcceptedItems)
            lastApplied = newAcceptedItems.maxOrDefault(lastApplied)
        }

    suspend fun proposeChange(change: Change, term: Int): Int {
        mutex.withLock {
            val newId = commitIndex
            proposedItems.add(LedgerItem(newId, term, change))
            commitIndex++
            return newId
        }
    }

    fun getHistory(): History {
        return history
    }

    suspend fun getAcceptedChanges(): List<Change> =
        mutex.withLock {
            acceptedItems.map { it.change }.toMutableList()
        }

    suspend fun getProposedChanges(): List<Change> =
        mutex.withLock {
            proposedItems.map { it.change }.toMutableList()
        }

    suspend fun getProposedChanges(ids: List<Int>): List<Change> =
        mutex.withLock {
            proposedItems
                .filter { ids.contains(it.ledgerIndex) }
                .map { it.change }.toMutableList()
        }


    suspend fun isChangeAccepted(change: Change): Boolean =
        mutex.withLock {
            acceptedItems.any { it.change == change }
        }

    suspend fun checkIfItemExist(logIndex: Int, logTerm: Int): Boolean =
        mutex.withLock {
            acceptedItems
                .lastOrNull()
                ?.let { it.ledgerIndex == logIndex && it.term == logTerm } ?: false
        }

    suspend fun removeNotAcceptedItems(logIndex: Int, logTerm: Int) {
        mutex.withLock {
            proposedItems.removeAll { it.ledgerIndex > logIndex || it.term > logTerm }
            acceptedItems.removeAll { it.ledgerIndex > logIndex || it.term > logTerm }
        }
    }

    suspend fun getLastAppliedChangeIdAndTermBeforeIndex(index: Int): Pair<Int, Int>? =
        mutex.withLock {
            acceptedItems
                .sortedBy { it.ledgerIndex }
                .lastOrNull { it.ledgerIndex <= index }
                ?.let { Pair(it.ledgerIndex, it.term) }
        }

    suspend fun changeAlreadyProposed(change: Change): Boolean =
        mutex.withLock {
            (acceptedItems + proposedItems)
                .any { it.change.toHistoryEntry() == change.toHistoryEntry() }
        }

    suspend fun getLedgerIdByChange(change: Change): Int =
        mutex.withLock {
            (acceptedItems + proposedItems)
                .find { it.change.toHistoryEntry() == change.toHistoryEntry() }!!.ledgerIndex
        }


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

data class LedgerUpdateResult(val anyAcceptedChange: Boolean, val anyProposedChange: Boolean)
