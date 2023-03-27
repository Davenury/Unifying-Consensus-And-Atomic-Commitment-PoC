package com.github.davenury.common.history

import com.github.davenury.common.meterRegistry
import com.zopa.ktor.opentracing.span

class MeteredHistory(
    private val delegate: History
) : History {
    override fun getCurrentEntryId(): String = span {
        meterRegistry.timer("history_get_current_entry_id").record<String> { delegate.getCurrentEntryId() }!!
    }

    override fun getCurrentEntry(): HistoryEntry = span {
        meterRegistry.timer("history_get_current_entry").record<HistoryEntry> { delegate.getCurrentEntry() }!!
    }

    override fun addEntry(entry: HistoryEntry) = span {
        meterRegistry.timer("history_add_entry").record {
            try {
                delegate.addEntry(entry)
            } catch (e: HistoryException) {
                meterRegistry.counter("history_incorrect_entry").increment()
                throw e
            }
        }
    }

    override fun getEntry(id: String): HistoryEntry = span {
        meterRegistry.timer("history_get_entry").record<HistoryEntry> { delegate.getEntry(id) }!!
    }

    override fun getEntryFromHistory(id: String): HistoryEntry? = span {
        meterRegistry.timer("history_get_entry_from_history").record<HistoryEntry?> { delegate.getEntryFromHistory(id) }
    }

    override fun toEntryList(skipInitial: Boolean): List<HistoryEntry> = span {
        meterRegistry.timer("history_to_entry_list").record<List<HistoryEntry>> { delegate.toEntryList(skipInitial) }!!
    }

    override fun containsEntry(entryId: String): Boolean = span {
        meterRegistry.timer("history_contains_entry").record<Boolean> { delegate.containsEntry(entryId) }!!
    }

    override fun isEntryCompatible(entry: HistoryEntry): Boolean = span {
        meterRegistry.timer("history_is_entry_compatible").record<Boolean> { delegate.isEntryCompatible(entry) }!!
    }

    override fun getAllEntriesUntilHistoryEntryId(historyEntryId: String): List<HistoryEntry> = span {
        meterRegistry.timer("history_get_all_entries_until_history").record<List<HistoryEntry>> {
            delegate.getAllEntriesUntilHistoryEntryId(historyEntryId)
        }!!
    }
}
