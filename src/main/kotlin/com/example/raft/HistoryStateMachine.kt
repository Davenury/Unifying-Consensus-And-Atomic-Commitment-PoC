package com.example.raft

import com.example.domain.Change
import com.example.objectMapper
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper

typealias History = MutableList<Change>


class HistoryStateMachine(override var state: History = mutableListOf()) : StateMachine<History>(state) {

    override fun serializeState(): String =
        objectMapper.writeValueAsString(state)

    override fun applyOperation(operation: String): String? =
        try {
            val change: Change = Change.fromJson(operation)
            state.add(change)
            null
        } catch (e: JsonMappingException) {
            e.message
        }


    override fun queryOperation(operation: String): String =
        try {
            when (OperationType.valueOf(operation)) {
                OperationType.LAST -> state.last().let { objectMapper.writeValueAsString(it) }
                OperationType.STATE -> serializeState()
            }
        } catch (e: java.lang.IllegalArgumentException) {
            "INVALID_OPERATION"
        }

    enum class OperationType {
        LAST,
        STATE,
    }
}