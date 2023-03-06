package com.github.davenury.common.history

import java.lang.IllegalArgumentException
import java.lang.NumberFormatException
import java.lang.StringBuilder

/**
 * @author Kamil Jarosz
 */
internal class EntryId private constructor(private val parts: LongArray) {
    companion object {
        fun fromString(id: String): EntryId {
            if (id.length != 128) {
                throw IllegalArgumentException("Wrong entry ID: $id")
            }

            val parts = LongArray(8)
            for (i in 0 until 8) {
                val partString = id.substring(i * 16, (i + 1) * 16)
                try {
                    parts[i] = java.lang.Long.parseUnsignedLong(partString, 16)
                } catch (e: NumberFormatException) {
                    throw IllegalArgumentException("Wrong entry ID: $id", e)
                }
            }
            return EntryId(parts)
        }
    }

    override fun toString(): String {
        val str = StringBuilder()
        for (i in 0 until 8) {
            str.append(java.lang.Long.toUnsignedString(parts[i], 16))
        }
        return str.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EntryId

        if (!parts.contentEquals(other.parts)) return false

        return true
    }

    override fun hashCode(): Int {
        return parts.contentHashCode()
    }
}
