package com.example.consensus.ratis

import com.example.common.Change
import com.example.common.ChangeWithAcceptNum
import com.example.common.History
import com.example.objectMapper
import com.fasterxml.jackson.databind.JsonMappingException
import org.slf4j.LoggerFactory


class HistoryStateMachine(override var state: History = mutableListOf()) :
        StateMachine<History>(state) {

    override fun serializeState(): String = objectMapper.writeValueAsString(state)

    override fun applyOperation(operation: String): String? =
            try {
                state.add(ChangeWithAcceptNum.fromJson(operation))
                null
            } catch (e: JsonMappingException) {
                logger.error("Error during applyOperation ${e.message}")
                e.message
            }

    override fun queryOperation(operation: String): String =
            try {
                OperationType.valueOf(operation).invokeOperation(state)
            } catch (e: java.lang.IllegalArgumentException) {
                logger.error("Error during queryOperation: illegalArgumentException")
                "INVALID_OPERATION"
            }

    enum class OperationType {
        LAST {
            override fun invokeOperation(state: History): String {
                return state.last().let { objectMapper.writeValueAsString(it) }
            }
        },
        STATE {
            override fun invokeOperation(state: History): String {
                return objectMapper.writeValueAsString(state)
            }
        };

        abstract fun invokeOperation(state: History): String
    }

    companion object {
        private val logger = LoggerFactory.getLogger(HistoryStateMachine::class.java)
    }
}
