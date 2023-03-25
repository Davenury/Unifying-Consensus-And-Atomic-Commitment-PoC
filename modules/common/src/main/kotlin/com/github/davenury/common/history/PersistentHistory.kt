package com.github.davenury.common.history

import com.github.davenury.common.persistence.Persistence
import org.slf4j.LoggerFactory

private const val CURRENT_ENTRY_ID = "current_entry_id"
private const val ENTRY_ID_PREFIX = "entry/"

/**
 * @author Kamil Jarosz
 */
class PersistentHistory(private val persistence: Persistence) : CachedHistory() {
    init {
        val initial = InitialHistoryEntry

        persistEntry(initial)

        val currentEntryId = persistence.get(CURRENT_ENTRY_ID)
        if (currentEntryId == null) {
            val id = initial.getId()
            logger.debug("No current entry, setting $id")
            persistence.set(CURRENT_ENTRY_ID, id)
        } else {
            logger.debug("Current entry ID is $currentEntryId")
        }
    }

    private fun persistEntry(entry: HistoryEntry) {
        val key = "$ENTRY_ID_PREFIX${entry.getId()}"
        logger.trace("Persisting entry ${entry.getId()} as $key")
        persistence.set(key, entry.serialize())
    }

    override fun getCurrentEntryId(): String {
        val currentEntryId = persistence.get(CURRENT_ENTRY_ID)!!
        logger.trace("Current entry ID is $currentEntryId")
        return currentEntryId
    }

    private fun compareAndSetCurrentEntryId(expected: String, new: String): Boolean {
        return persistence.compareAndExchange(CURRENT_ENTRY_ID, expected, new) == expected
    }

    override fun getCurrentEntry(): HistoryEntry {
        return getEntry(getCurrentEntryId())
    }

    override fun getEntry(id: String): HistoryEntry {
        val serialized = persistence.get("$ENTRY_ID_PREFIX${id}")
        return serialized?.let { HistoryEntry.deserialize(it) }
            ?: throw EntryNotFoundException(
                "Entry $id not present in entries"
            )
    }

    override fun addEntry(entry: HistoryEntry) {
        val newId = entry.getId()
        val expectedParentId = getCurrentEntryId()

        if (entry.getParentId() != expectedParentId) {
            throw HistoryException(
                "Wrong parent ID, expected ${expectedParentId}, " +
                        "got ${entry.getParentId()}, entryId=${newId}"
            )
        }

        persistEntry(entry)

        val successful = compareAndSetCurrentEntryId(expectedParentId, newId)
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
