package com.github.davenury.ucac.history

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * @author Kamil Jarosz
 */
class History {
    private val initialEntry: InitialHistoryEntry = InitialHistoryEntry
    private val currentEntryId: AtomicReference<String> = AtomicReference(initialEntry.getId())
    private val entries: ConcurrentHashMap<String, HistoryEntry> = ConcurrentHashMap()

    init {
        entries[initialEntry.getId()] = initialEntry
    }

    fun getCurrentEntry(): HistoryEntry {
        return getEntry(currentEntryId.get())
    }

    private fun getEntry(id: String): HistoryEntry {
        return entries[id] ?: throw AssertionError(
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
            throw HistoryException(
                "Optimistic locking exception: parent changed concurrently, " +
                        "entryId=${newId}"
            )
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
}
