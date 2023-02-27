package com.github.davenury.common.history

import com.github.davenury.common.meterRegistry

class MeteredHistory(
    private val delegate: History
) : History {
    override fun getCurrentEntry(): HistoryEntry =
        meterRegistry.timer("history_get_current_entry").record<HistoryEntry> { delegate.getCurrentEntry() }!!

    override fun addEntry(entry: HistoryEntry) =
        meterRegistry.timer("history_add_entry").record { delegate.addEntry(entry) }

    override fun getEntry(id: String): HistoryEntry =
        meterRegistry.timer("history_get_entry").record<HistoryEntry> { delegate.getEntry(id) }!!

    override fun getEntryFromHistory(id: String): HistoryEntry? =
        meterRegistry.timer("history_get_entry_from_history").record<HistoryEntry?> { delegate.getEntryFromHistory(id) }

    override fun toEntryList(skipInitial: Boolean): List<HistoryEntry> =
        meterRegistry.timer("history_to_entry_list").record<List<HistoryEntry>> { delegate.toEntryList(skipInitial) }!!

    override fun containsEntry(entryId: String): Boolean =
        meterRegistry.timer("history_contains_entry").record<Boolean> { delegate.containsEntry(entryId) }!!

    override fun isEntryCompatible(entry: HistoryEntry): Boolean =
        meterRegistry.timer("history_is_entry_compatible").record<Boolean> { delegate.isEntryCompatible(entry) }!!

    override fun getAllEntriesUntilHistoryEntryId(historyEntryId: String): List<HistoryEntry> =
        meterRegistry.timer("history_get_all_entries_until_history").record<List<HistoryEntry>> {
            delegate.getAllEntriesUntilHistoryEntryId(historyEntryId)
        }!!
}