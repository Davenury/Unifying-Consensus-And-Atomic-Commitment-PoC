package com.github.davenury.ucac.common

import com.github.davenury.common.history.*
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.PersistenceType

/**
 * @author Kamil Jarosz
 */
class HistoryFactory {
    fun createForConfig(config: Config): History {
        val persistence = config.persistence
        val history = when (persistence.type) {
            PersistenceType.IN_MEMORY -> MeteredHistory(InMemoryHistory())
            PersistenceType.REDIS -> {
                MeteredHistory(JedisHistory(persistence.redisHost!!, persistence.redisPort!!))
            }
        }
        return CachedHistory(history)
    }
}
