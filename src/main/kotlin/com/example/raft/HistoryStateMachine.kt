package com.example.raft

import com.example.domain.Change
import com.example.objectMapper
import com.fasterxml.jackson.databind.JsonMappingException
import org.slf4j.LoggerFactory

data class ChangeWithAcceptNum(val change: Change, val acceptNum: Int?)

typealias History = MutableList<ChangeWithAcceptNum>

class HistoryStateMachine(override var state: History = mutableListOf()) :
        StateMachine<History>(state) {

    override fun serializeState(): String = objectMapper.writeValueAsString(state)

    override fun applyOperation(operation: String): String? =
            try {
                val map = objectMapper.readValue(operation, HashMap<String, Any>().javaClass)
                val changeString = objectMapper.writeValueAsString(map["change"])
                val change: Change = Change.fromJson(changeString)!!
                val acceptNum = map["acceptNum"] as Int?
                state.add(ChangeWithAcceptNum(change, acceptNum))
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
