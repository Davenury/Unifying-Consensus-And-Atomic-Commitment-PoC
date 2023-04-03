package com.github.davenury.common.persistence

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPooled

private const val CAE_SCRIPT = """
    local witness = redis.call('GET', KEYS[1])
    if witness == ARGV[1] then
        redis.call('SET', KEYS[1], ARGV[2])
    end

    return witness
"""

/**
 * @author Kamil Jarosz
 */
class RedisPersistence(host: String, port: Int): Persistence {
    private val jedis: JedisPooled = JedisPooled(host, port)
    private val caeSha: String

    init{
        logger.info("Using Redis for persistence at $host:$port")
        caeSha = jedis.scriptLoad(CAE_SCRIPT, null)
        logger.debug("Loaded CAE $caeSha")
    }

    override fun set(key: String, value: String) {
        jedis.set(key, value)
    }

    override fun get(key: String): String? {
        return jedis.get(key)
    }

    override fun compareAndExchange(key: String, expected: String, new: String): String? {
        val witness = jedis.evalsha(
            caeSha,
            listOf(key),
            listOf(expected, new)
        )
        logger.trace("CAE expected: $expected, new: $new -> witness: $witness")
        return witness as String?
    }

    companion object {
        private val logger = LoggerFactory.getLogger("redis")
    }
}
