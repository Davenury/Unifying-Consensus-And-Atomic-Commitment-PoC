package com.github.davenury.ucac.common

import com.github.davenury.common.persistence.InMemoryPersistence
import com.github.davenury.common.persistence.Persistence
import com.github.davenury.common.persistence.RedisPersistence
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.PersistenceType

/**
 * @author Kamil Jarosz
 */
class PersistenceFactory {
    fun createForConfig(config: Config): Persistence {
        val persistenceConfig = config.persistence
        return when (persistenceConfig.type) {
            PersistenceType.IN_MEMORY -> InMemoryPersistence()
            PersistenceType.REDIS -> {
                RedisPersistence(persistenceConfig.redisHost!!, persistenceConfig.redisPort!!)
            }
        }
    }
}
