package com.github.davenury.common.history

import com.github.davenury.common.Metrics
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPooled

private const val CURRENT_ENTRY_ID = "current_entry_id"
private const val ENTRY_ID_PREFIX = "entry/"

private const val CAS_SCRIPT = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('SET', KEYS[1], ARGV[2])
        return true
    else
        return false
    end
"""

/**
 * @author Kamil Jarosz
 */
class JedisHistory(host: String, port: Int) : CachedHistory() {
    private val jedis: JedisPooled = JedisPooled(host, port)
    private val casSha: String

    init {
        logger.info("Using Redis for history at $host:$port")
        val initial = InitialHistoryEntry

        casSha = jedis.scriptLoad(CAS_SCRIPT, null)
        logger.debug("Loaded CAS $casSha")

        persistEntry(initial)

        val currentEntryId = jedis.get(CURRENT_ENTRY_ID)
        if (currentEntryId == null) {
            val id = initial.getId()
            logger.debug("No current entry, setting $id")
            jedis.set(CURRENT_ENTRY_ID, id)
        } else {
            logger.debug("Current entry ID is $currentEntryId")
        }
    }

    private fun persistEntry(entry: HistoryEntry) {
        val key = "$ENTRY_ID_PREFIX${entry.getId()}"
        logger.trace("Persisting entry ${entry.getId()} as $key")
        jedis.set(key, entry.serialize())
    }

    private fun getCurrentEntryId(): String {
        val currentEntryId = jedis.get(CURRENT_ENTRY_ID)
        logger.trace("Current entry ID is $currentEntryId")
        return currentEntryId
    }

    private fun compareAndSetCurrentEntryId(expected: String, new: String): Boolean {
        val result = jedis.evalsha(
            casSha,
            listOf(CURRENT_ENTRY_ID),
            listOf(expected, new)
        )
        logger.trace("CAS expected: $expected, new: $new -> result: $result")
        return 1L == result
    }

    override fun getCurrentEntry(): HistoryEntry {
        return getEntry(getCurrentEntryId())
    }

    override fun getEntry(id: String): HistoryEntry {
        val serialized = jedis.get("$ENTRY_ID_PREFIX${id}")
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
