package com.github.davenury.common.history

import com.github.davenury.common.Metrics
import com.github.davenury.common.meterRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * @author Kamil Jarosz
 */
class InMemoryHistory : CachedHistory() {
    private val initialEntry: InitialHistoryEntry = InitialHistoryEntry
    private val currentEntryId: AtomicReference<String> = AtomicReference(initialEntry.getId())
    private val entries: ConcurrentHashMap<String, HistoryEntry> = ConcurrentHashMap()

    init {
        entries[initialEntry.getId()] = initialEntry
    }

    override fun getCurrentEntry(): HistoryEntry {
        return getEntry(currentEntryId.get())
    }

    override fun getEntry(id: String): HistoryEntry {
        return entries[id] ?: throw EntryNotFoundException(
            "Entry $id not present in entries"
        )
    }

    override fun addEntry(entry: HistoryEntry) {
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
        } else {
            logger.info("History entry added ($newId): $entry")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("history")
    }
}
