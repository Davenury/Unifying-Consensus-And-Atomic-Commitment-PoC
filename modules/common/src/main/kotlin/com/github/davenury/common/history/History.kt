package com.github.davenury.common.history

import org.slf4j.LoggerFactory
import com.github.davenury.common.Metrics
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * @author Kamil Jarosz
 */
class History {
    private val initialEntry: InitialHistoryEntry = InitialHistoryEntry
    private val currentEntryId: AtomicReference<String> = AtomicReference(initialEntry.getId())
    private val entries: ConcurrentHashMap<String, HistoryEntry> = ConcurrentHashMap()
    private val ancestors: ConcurrentHashMap<String, Set<String>> = ConcurrentHashMap()

    init {
        entries[initialEntry.getId()] = initialEntry
    }

    fun getCurrentEntry(): HistoryEntry {
        return getEntry(currentEntryId.get())
    }

    @Throws(EntryNotFoundException::class)
    private fun getAncestors(entryId: String): Set<String> {
        return ancestors.computeIfAbsent(entryId) { id ->
            val entry = getEntry(id)
            val parentId = entry.getParentId()
            return@computeIfAbsent if (parentId == null) {
                setOf(id)
            } else {
                getAncestors(parentId).union(setOf(id))
            }
        };
    }

    @Throws(EntryNotFoundException::class)
    private fun getEntry(id: String): HistoryEntry {
        return entries[id] ?: throw EntryNotFoundException(
            "Entry from history not present in entries: $id"
        )
    }

    fun addEntry(entry: HistoryEntry) {
        val newId = entry.getId()
        val expectedParentId = currentEntryId.get()

        if (entry.getParentId() != expectedParentId) {
            throw HistoryException(
                "Wrong parent ID, expected ${expectedParentId}, " +
                        "got ${entry.getParentId()}, entryId=${newId}"
            )
        }

        val existing = entries.put(newId, entry) != null
        var successful = false
        try {
            successful = currentEntryId.compareAndSet(expectedParentId, newId)
        } finally {
            if (!successful && !existing) {
                entries.remove(newId)
            }
        }

        if (!successful) {
            Metrics.bumpIncorrectHistory()
            throw HistoryException(
                "Optimistic locking exception: parent changed concurrently, " +
                        "entryId=${newId}"
            )
        } else {
            logger.info("History entry added ($newId): $entry")
        }
    }

    fun getEntryFromHistory(id: String): HistoryEntry? {
        var entry = getCurrentEntry()
        while (true) {
            if (entry.getId() == id) {
                return entry
            }

            if (entry.getParentId() == null) {
                return null
            } else {
                entry = getEntry(entry.getParentId()!!)
            }
        }
    }

    fun toEntryList(skipInitial: Boolean = true): List<HistoryEntry> {
        val list = ArrayList<HistoryEntry>()
        var entry = getCurrentEntry()
        while (true) {
            if (skipInitial && entry == InitialHistoryEntry) {
                return list
            }
            list.add(entry)
            val parentId = entry.getParentId() ?: return list
            entry = getEntryFromHistory(parentId)!!
        }
    }

    fun containsEntry(entryId: String): Boolean {
        return getAncestors(currentEntryId.get()).contains(entryId)
    }

    fun isEntryCompatible(entry: HistoryEntry): Boolean {
        return containsEntry(entry.getId()) || getCurrentEntry().getId() == entry.getParentId()
    }

    fun getAllEntriesUntilHistoryEntryId(historyEntryId: String): List<HistoryEntry> =
        if(containsEntry(historyEntryId)){
            val resultList: MutableList<HistoryEntry> = mutableListOf()
            var entry = getCurrentEntry()
            while(entry.getId() != historyEntryId){
                resultList.add(entry)
                entry = getEntry(entry.getParentId()!!)
            }

            resultList.reversed()
        }else{
            listOf<HistoryEntry>()
        }

    companion object {
        private val logger = LoggerFactory.getLogger("history")
    }
}
