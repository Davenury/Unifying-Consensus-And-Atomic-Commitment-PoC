package com.github.davenury.ucac.history

import com.fasterxml.jackson.databind.ObjectMapper
import java.lang.IllegalArgumentException

interface HistoryEntry {
    fun getId(): String

    fun getParentId(): String?

    fun getContent(): String

    fun serialize(): String

    companion object {
        fun deserialize(serialized: String): HistoryEntry {
            val map = ObjectMapper().readValue(serialized, HashMap::class.java)
            val entry =
                if (map.containsKey("parentId")) {
                    IntermediateHistoryEntry(map["content"] as String, map["parentId"] as String)
                } else {
                    InitialHistoryEntry
                }
            if (entry.serialize() != serialized) {
                throw IllegalArgumentException("Invalid data: $serialized")
            }
            return entry
        }
    }
}
