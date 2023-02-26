package com.github.davenury.common.history

import com.github.davenury.common.meterRegistry
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Kamil Jarosz
 */
class CachedHistory(private val delegate: History) : History {
    private val ancestors: ConcurrentHashMap<String, Set<String>> = ConcurrentHashMap()

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

    override fun getEntryFromHistory(id: String): HistoryEntry? {
        return if (containsEntry(id)) {
            getEntry(id)
        } else {
            null
        }
    }

    override fun getCurrentEntry(): HistoryEntry = delegate.getCurrentEntry()

    override fun getEntry(id: String): HistoryEntry = delegate.getEntry(id)

    override fun addEntry(entry: HistoryEntry) = meterRegistry.timer("in_memory_history_add_entry").record {
        delegate.addEntry(entry)
    }

    override fun containsEntry(entryId: String): Boolean {
        return getAncestors(getCurrentEntry().getId()).contains(entryId)
    }

    override fun toEntryList(skipInitial: Boolean): List<HistoryEntry> =
        delegate.toEntryList(skipInitial)

    override fun getAllEntriesUntilHistoryEntryId(historyEntryId: String): List<HistoryEntry> =
        delegate.getAllEntriesUntilHistoryEntryId(historyEntryId)

    override fun isEntryCompatible(entry: HistoryEntry): Boolean =
        meterRegistry.timer("history_is_entry_compatible").record<Boolean> {
            delegate.isEntryCompatible(entry)
        }!!
}
