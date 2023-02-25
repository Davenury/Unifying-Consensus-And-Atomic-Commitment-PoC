package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.InitialHistoryEntry
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock


data class Ledger(
    private val history: History,
    val proposedEntries: MutableList<LedgerItem> = mutableListOf(),
    private val mutex: Mutex = Mutex(),
) {

    var commitIndex: String = InitialHistoryEntry.getId()
    var lastApplied: String = InitialHistoryEntry.getId()

    suspend fun updateLedger(leaderCommitHistoryEntryId: String, proposedItems: List<LedgerItem>): LedgerUpdateResult =
        mutex.withLock {
            this.proposedEntries.addAll(proposedItems)

            val newAcceptedItems = updateCommitIndex(leaderCommitHistoryEntryId)

            return LedgerUpdateResult(
                acceptedItems = newAcceptedItems,
                proposedItems = proposedItems,
            )
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
                entries + proposedEntries
            else
                proposedEntries.dropWhile { it.entry.getId() != historyEntryId }.drop(1)

        }

    private suspend fun updateCommitIndex(commitHistoryEntryId: String): List<LedgerItem> {
        this.commitIndex = commitHistoryEntryId
        if (lastApplied == commitIndex) return listOf()


        val index = proposedEntries.indexOfFirst { it.entry.getId() == commitIndex }
        val newAcceptedItems = proposedEntries.take(index + 1)

        newAcceptedItems.forEach {
            history.addEntry(it.entry)
            proposedEntries.remove(it)
            lastApplied = it.entry.getId()
        }

        return newAcceptedItems
    }

    suspend fun acceptItems(acceptedEntriesIds: List<String>) = mutex.withLock {
        acceptedEntriesIds
            .map { entryId -> proposedEntries.indexOfFirst { it.entry.getId() == entryId } }
            .maxOfOrNull { it }
            ?.let { updateCommitIndex(proposedEntries.elementAt(it).entry.getId()) }
    }

    suspend fun proposeEntry(entry: HistoryEntry, changeId: String) =
        mutex.withLock {
            proposedEntries.add(LedgerItem(entry, changeId))
        }

    fun getHistory(): History {
        return history
    }

    suspend fun getLogEntries(): List<LedgerItem> =
        mutex.withLock {
            proposedEntries
        }

    suspend fun getLogEntries(historyEntryIds: List<String>): List<LedgerItem> =
        mutex.withLock {
            proposedEntries.filter { historyEntryIds.contains(it.entry.getId()) }
        }

    suspend fun checkIfItemExist(historyEntryId: String): Boolean =
        mutex.withLock {
            proposedEntries
                .find { it.entry.getId() == historyEntryId }
                ?.let { true }
                ?: history.containsEntry(historyEntryId)
        }

    suspend fun checkIfProposedItemsAreStillValid() =
        mutex.withLock {
            val newProposedItems = proposedEntries.fold(listOf<LedgerItem>()) { acc, ledgerItem ->
                if (acc.isEmpty() && history.isEntryCompatible(ledgerItem.entry)) acc.plus(ledgerItem)
                else if (acc.isNotEmpty() && acc.last().entry.getId() == ledgerItem.entry.getParentId()) acc.plus(
                    ledgerItem
                )
                else acc
            }
            proposedEntries.removeAll { newProposedItems.contains(it) }
        }

    suspend fun removeNotAcceptedItems() =
        mutex.withLock {
            proposedEntries.clear()
        }

    suspend fun entryAlreadyProposed(entry: HistoryEntry): Boolean =
        mutex.withLock {
            proposedEntries.any { it.entry == entry }
        }

    suspend fun getPreviousEntryId(entryId: String): String = mutex.withLock {
        if (history.containsEntry(entryId))
            history.getEntryFromHistory(entryId)?.getParentId() ?: InitialHistoryEntry.getId()
        else {
            proposedEntries
                .find { it.entry.getId() == entryId }
                ?.entry
                ?.getParentId()!!
        }
    }

    suspend fun isNotApplied(entryId: String): Boolean = mutex.withLock { !history.containsEntry(entryId) }
    suspend fun isNotAppliedNorProposed(entryId: String): Boolean =
        mutex.withLock { !history.containsEntry(entryId) && !proposedEntries.any { it.entry.getId() == entryId } }

    suspend fun checkCommitIndex() = mutex.withLock {
        val currentEntryId = this.history.getCurrentEntry().getId()
        if (currentEntryId != commitIndex) {
            commitIndex = currentEntryId
            lastApplied = currentEntryId
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
