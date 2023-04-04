package com.github.davenury.common.persistence

/**
 * @author Kamil Jarosz
 */
interface Persistence {
    fun set(key: String, value: String)
    fun get(key: String): String?
    fun compareAndExchange(key: String, expected: String, new: String): String?
}
