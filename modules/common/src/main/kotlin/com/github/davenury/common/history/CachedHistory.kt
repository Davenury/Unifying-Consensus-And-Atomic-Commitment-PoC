package com.github.davenury.common.history

<<<<<<< HEAD
import java.util.*
=======
import com.github.davenury.common.meterRegistry
>>>>>>> 0cb27e1 (timer on history operations)
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.HashSet

/**
 * @author Kamil Jarosz
 */
abstract class CachedHistory : History {
    private val ancestors: ConcurrentHashMap<String, Set<String>> = ConcurrentHashMap()

    @Throws(EntryNotFoundException::class)
    private fun getAncestors(entryId: String): Set<String> {
        val existing = ancestors[entryId]
        if (existing != null) {
            return existing
        }

        val entry = getEntry(entryId)
        val parentId = entry.getParentId()

        val new = if (parentId == null) {
            Collections.singleton(entryId)
        } else {
            val union = HashSet<String>(getAncestors(parentId))
            union.add(entryId)
            Collections.unmodifiableSet(union)
        }
        ancestors.putIfAbsent(entryId, new)
        return new
    }

    override fun getEntryFromHistory(id: String): HistoryEntry? {
        return if (containsEntry(id)) {
            getEntry(id)
        } else {
            null
        }
    }
    
    override fun containsEntry(entryId: String): Boolean {
        return getAncestors(getCurrentEntry().getId()).contains(entryId)
    }

}
