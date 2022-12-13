package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
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

            val newProposedItems = proposedItems - this.proposedItems.toSet()
            this.proposedItems.removeAll { acceptedIds.contains(it.ledgerIndex) }
            val newProposedLedgerIndex = proposedItems.map { it.ledgerIndex }
            this.proposedItems.removeAll { newProposedLedgerIndex.contains(it.ledgerIndex) }
            this.proposedItems.addAll(newProposedItems)
            commitIndex = newProposedItems.maxOrDefault(commitIndex)

            newAcceptedItems.forEach { history.addEntry(it.entry) }
            return LedgerUpdateResult(
                acceptedItems = newAcceptedItems,
                proposedItems = newProposedItems,
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
            newAcceptedItems.forEach { history.addEntry(it.entry) }
            proposedItems.removeAll(newAcceptedItems)
            lastApplied = newAcceptedItems.maxOrDefault(lastApplied)
        }

    suspend fun proposeEntry(entry: HistoryEntry, term: Int, changeId: String): Int {
        mutex.withLock {
            val newId = commitIndex
            proposedItems.add(LedgerItem(newId, term, entry, changeId))
            commitIndex++
            return newId
        }
    }

    fun getHistory(): History {
        return history
    }

    suspend fun getAcceptedItems(): List<LedgerItem> =
        mutex.withLock {
            acceptedItems.map { it }.toList()
        }

    suspend fun getProposedItems(): List<LedgerItem> =
        mutex.withLock {
            proposedItems.map { it }.toList()
        }

    suspend fun getProposedItems(ids: List<Int>): List<LedgerItem> =
        mutex.withLock {
            proposedItems
                .filter { ids.contains(it.ledgerIndex) }
                .map { it }.toList()
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

    suspend fun entryAlreadyProposed(entry: HistoryEntry): Boolean =
        mutex.withLock {
            (acceptedItems + proposedItems)
                .any { it.entry == entry }
        }

    private fun List<LedgerItem>.maxOrDefault(defaultValue: Int): Int =
        this.maxOfOrNull { it.ledgerIndex } ?: defaultValue

    @JvmName("maxOrDefaultInt")
    private fun List<Int>.maxOrDefault(defaultValue: Int): Int = this.maxOfOrNull { it } ?: defaultValue
}

data class LedgerItemDto(val ledgerIndex: Int, val term: Int, val serializedEntry: String, val changeId: String) {
    fun toLedgerItem(): LedgerItem = LedgerItem(ledgerIndex, term, HistoryEntry.deserialize(serializedEntry), changeId)
}

data class LedgerItem(val ledgerIndex: Int, val term: Int, val entry: HistoryEntry, val changeId: String) {
    fun toDto(): LedgerItemDto = LedgerItemDto(ledgerIndex, term, entry.serialize(), changeId)
}

data class LedgerUpdateResult(val acceptedItems: List<LedgerItem>, val proposedItems: List<LedgerItem>)
