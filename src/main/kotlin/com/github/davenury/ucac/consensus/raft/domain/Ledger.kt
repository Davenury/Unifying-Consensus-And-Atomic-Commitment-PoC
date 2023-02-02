package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock


data class Ledger(
    private val history: History,
    val logEntries: MutableList<LedgerItem> = mutableListOf(),
    private val mutex: Mutex = Mutex(),
) {

    var commitIndex: Int = -1
    var lastApplied = -1

    suspend fun updateLedger(leaderCommitIndex: Int, proposedItems: List<LedgerItem>): LedgerUpdateResult {
        mutex.withLock {

            val proposedItemsIndex = proposedItems.map { it.ledgerIndex }

            this.logEntries.removeAll { proposedItemsIndex.contains(it.ledgerIndex) }
            this.logEntries.addAll(proposedItems)

            val newAcceptedItems = updateCommitIndex(leaderCommitIndex)


            return LedgerUpdateResult(
                acceptedItems = newAcceptedItems,
                proposedItems = proposedItems,
            )
        }
    }

    suspend fun getNewProposedItems(ledgerIndex: Int) =
        mutex.withLock {
            logEntries.filter { it.ledgerIndex > ledgerIndex }
        }

    private fun updateCommitIndex(commitIndex: Int): List<LedgerItem> {
        var newAcceptedItems = listOf<LedgerItem>()
        this.commitIndex = commitIndex
        if (lastApplied < this.commitIndex) {
            newAcceptedItems = logEntries
                .filter { it.ledgerIndex in (lastApplied + 1) until commitIndex + 1 }
            newAcceptedItems.forEach { history.addEntry(it.entry) }
            lastApplied = newAcceptedItems.maxOf { it.ledgerIndex }
        }
        return newAcceptedItems
    }

    suspend fun acceptItems(acceptedIndexes: List<Int>) =
        mutex.withLock {
            acceptedIndexes
                .maxOfOrNull { it }
                ?.let { updateCommitIndex(it) }
        }

    suspend fun proposeEntry(entry: HistoryEntry, term: Int, changeId: String): Int {
        mutex.withLock {
            val newId = (logEntries.lastOrNull()?.ledgerIndex ?: -1) + 1
            logEntries.add(LedgerItem(newId, term, entry, changeId))
            return newId
        }
    }

    fun getHistory(): History {
        return history
    }

    suspend fun getLogEntries(): List<LedgerItem> =
        mutex.withLock {
            logEntries.map { it }.toList()
        }

    suspend fun getLogEntries(ids: List<Int>): List<LedgerItem> =
        mutex.withLock {
            logEntries
                .filter { ids.contains(it.ledgerIndex) }
                .map { it }.toList()
        }

    suspend fun checkIfItemExist(logIndex: Int, logTerm: Int): Boolean =
        mutex.withLock {
            logEntries
                .find { it.ledgerIndex == logIndex && it.term == logTerm }
                ?.let { true } ?: false
        }

    suspend fun checkIfProposedItemsAreStillValid() =
        mutex.withLock {
            val newProposedItems = logEntries.fold(listOf<LedgerItem>()) { acc, ledgerItem ->
                if (acc.isEmpty() && history.isEntryCompatible(ledgerItem.entry)) acc.plus(ledgerItem)
                else if (acc.isNotEmpty() && acc.last().entry.getId() == ledgerItem.entry.getParentId()) acc.plus(
                    ledgerItem
                )
                else acc
            }
            logEntries.removeAll { newProposedItems.contains(it) }
        }

    suspend fun removeNotAcceptedItems(logIndex: Int, logTerm: Int) {
        mutex.withLock {
            logEntries.removeAll { it.ledgerIndex > logIndex || it.term > logTerm }
        }
    }

    suspend fun getLastAppliedChangeIdAndTermBeforeIndex(index: Int): Pair<Int, Int>? =
        mutex.withLock {
            logEntries
                .sortedBy { it.ledgerIndex }
                .lastOrNull { it.ledgerIndex <= index && it.ledgerIndex <= lastApplied }
                ?.let { Pair(it.ledgerIndex, it.term) }
        }

    suspend fun entryAlreadyProposed(entry: HistoryEntry): Boolean =
        mutex.withLock {
            logEntries.any { it.entry == entry }
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
