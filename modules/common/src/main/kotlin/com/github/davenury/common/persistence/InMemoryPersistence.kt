package com.github.davenury.common.persistence

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPooled
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Kamil Jarosz
 */
class InMemoryPersistence : Persistence {
    private val values: ConcurrentHashMap<String, String?> = ConcurrentHashMap()

    init {
        logger.info("Using in-memory persistence")
    }

    override fun set(key: String, value: String) {
        values[key] = value
    }

    override fun get(key: String): String? {
        return values[key]
    }

    override fun compareAndExchange(key: String, expected: String, new: String): String? {
        var witness: String? = null
        values.compute(key) { _, value ->
            witness = value
            if (witness == expected) {
                new
            } else {
                value
            }
        }
        return witness
    }

    companion object {
        private val logger = LoggerFactory.getLogger("jedis")
    }
}
