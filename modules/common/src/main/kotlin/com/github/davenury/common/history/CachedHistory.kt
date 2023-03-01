package com.github.davenury.common.history


import java.lang.IllegalArgumentException
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashSet

/**
 * @author Kamil Jarosz
 */
abstract class CachedHistory : History {
    internal val ancestors: ConcurrentHashMap<String, Set<EntryId>> = ConcurrentHashMap()

    @Throws(EntryNotFoundException::class)
    private fun getAncestors(entryId: String): Set<EntryId> {
        val existing = ancestors[entryId]
        if (existing != null) {
            return existing
        }

        val newAncestors = HashSet<EntryId>()
        newAncestors.add(EntryId.fromString(entryId))
        val entry = getEntry(entryId)

        var ancestorId = entry.getParentId()
        var ancestorCacheKey: String? = null
        while (ancestorId != null) {
            val ancestorCache = ancestors[ancestorId]
            if (ancestorCache != null) {
                ancestorCacheKey = ancestorId
                newAncestors.addAll(ancestorCache)
                ancestorId = null
            } else {
                newAncestors.add(EntryId.fromString(ancestorId))
                ancestorId = getEntry(ancestorId).getParentId()
            }
        }

        ancestors.putIfAbsent(entryId, newAncestors)
        if (ancestorCacheKey != null) {
            ancestors.remove(ancestorCacheKey)
        }
        return newAncestors
    }

    override fun getEntryFromHistory(id: String): HistoryEntry? {
        return if (containsEntry(id)) {
            getEntry(id)
        } else {
            null
        }
    }

    override fun containsEntry(entryId: String): Boolean {
        val id: EntryId
        try {
            id = EntryId.fromString(entryId)
        } catch (e: IllegalArgumentException) {
            return false
        }
        return getAncestors(getCurrentEntryId()).contains(id)
    }
}
