package com.github.davenury.ucac.common

import com.github.davenury.common.history.CachedHistory
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.JedisHistory
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.PersistenceType

/**
 * @author Kamil Jarosz
 */
class HistoryFactory {
    fun createForConfig(config: Config): History {
        val persistence = config.persistence
        val history = when (persistence.type) {
            PersistenceType.IN_MEMORY -> InMemoryHistory()
            PersistenceType.REDIS -> {
                JedisHistory(persistence.redisHost!!, persistence.redisPort!!)
            }
        }
        return CachedHistory(history)
    }
}
