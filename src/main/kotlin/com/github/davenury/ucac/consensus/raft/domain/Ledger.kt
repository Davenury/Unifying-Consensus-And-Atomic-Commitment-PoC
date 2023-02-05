package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.InitialHistoryEntry
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock


data class Ledger(
    private val history: History,
    val logEntries: MutableList<LedgerItem> = mutableListOf(),
    private val mutex: Mutex = Mutex(),
) {

    var commitIndex: String = InitialHistoryEntry.getId()
    var lastApplied: String = InitialHistoryEntry.getId()

    suspend fun updateLedger(leaderCommitHistoryEntryId: String, proposedItems: List<LedgerItem>): LedgerUpdateResult {
        mutex.withLock {
            this.logEntries.addAll(proposedItems)

            val newAcceptedItems = updateCommitIndex(leaderCommitHistoryEntryId)

            return LedgerUpdateResult(
                acceptedItems = newAcceptedItems,
                proposedItems = proposedItems,
            )
        }
    }

    suspend fun getNewProposedItems(historyEntryId: String): List<LedgerItem> =
        mutex.withLock {
            val entries =
                history.getAllEntriesUntilHistoryEntryId(historyEntryId)
                    .map {
                        val change = Change.fromHistoryEntry(it)
                        LedgerItem(it, change?.id ?: it.getId())
                    }

            if (history.containsEntry(historyEntryId))
                entries + logEntries
            else
                logEntries.dropWhile { it.entry.getId() != historyEntryId }.drop(1)

        }

    private fun updateCommitIndex(commitHistoryEntryId: String): List<LedgerItem> {
        var newAcceptedItems = listOf<LedgerItem>()
        this.commitIndex = commitHistoryEntryId
        if (lastApplied != this.commitIndex) {
            val index = logEntries.indexOfFirst { it.entry.getId() == commitIndex }
            newAcceptedItems = logEntries.take(index + 1)
            newAcceptedItems.forEach { history.addEntry(it.entry) }
            logEntries.removeAll(newAcceptedItems)
            lastApplied = commitIndex
        }
        return newAcceptedItems
    }

    suspend fun acceptItems(acceptedEntriesIds: List<String>) =
        mutex.withLock {
            acceptedEntriesIds
                .map { entryId -> logEntries.indexOfFirst { it.entry.getId() == entryId } }
                .maxOfOrNull { it }
                ?.let { updateCommitIndex(logEntries.elementAt(it).entry.getId()) }
        }

    suspend fun proposeEntry(entry: HistoryEntry, changeId: String) {
        mutex.withLock {
            logEntries.add(LedgerItem(entry, changeId))
        }
    }

    fun getHistory(): History {
        return history
    }

    suspend fun getLogEntries(): List<LedgerItem> =
        mutex.withLock {
            logEntries.map { it }.toList()
        }

    suspend fun getLogEntries(historyEntryIds: List<String>): List<LedgerItem> =
        mutex.withLock {
            logEntries
                .filter { historyEntryIds.contains(it.entry.getId()) }
                .map { it }
                .toList()
        }

    suspend fun checkIfItemExist(historyEntryId: String): Boolean =
        mutex.withLock {
            logEntries
                .find { it.entry.getId() == historyEntryId }
                ?.let { true }
                ?: history.containsEntry(historyEntryId)
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

    suspend fun removeNotAcceptedItems() {
        mutex.withLock {
            logEntries.removeAll { true }
        }
    }

    suspend fun getLastAppliedChangeIdBeforeChangeId(historyEntryId: String): String =
        mutex.withLock {
            logEntries.find { it.entry.getId() == historyEntryId }
                ?.entry
                ?.getParentId()
                ?: history.getEntryFromHistory(historyEntryId)
                    ?.getParentId()
                ?: InitialHistoryEntry.getId()
        }

    suspend fun entryAlreadyProposed(entry: HistoryEntry): Boolean =
        mutex.withLock {
            logEntries.any { it.entry == entry }
        }

    fun getPreviousEntryId(entryId: String): String =
        if (history.containsEntry(entryId))
            history.getEntryFromHistory(entryId)?.getParentId() ?: InitialHistoryEntry.getId()
        else {
            logEntries
                .find { it.entry.getId() == entryId }
                ?.entry
                ?.getParentId()!!
        }

    fun isNotApplied(entryId: String): Boolean = !history.containsEntry(entryId)
    suspend fun checkCommitIndex() {
        mutex.withLock {
            val currentEntryId = this.history.getCurrentEntry().getId()
            if ( currentEntryId != commitIndex){
                commitIndex = currentEntryId
                lastApplied = currentEntryId
            }
        }

    }
}

data class LedgerItemDto(val serializedEntry: String, val changeId: String) {
    fun toLedgerItem(): LedgerItem = LedgerItem(HistoryEntry.deserialize(serializedEntry), changeId)
}

data class LedgerItem(val entry: HistoryEntry, val changeId: String) {
    fun toDto(): LedgerItemDto = LedgerItemDto(entry.serialize(), changeId)
}

data class LedgerUpdateResult(val acceptedItems: List<LedgerItem>, val proposedItems: List<LedgerItem>)
