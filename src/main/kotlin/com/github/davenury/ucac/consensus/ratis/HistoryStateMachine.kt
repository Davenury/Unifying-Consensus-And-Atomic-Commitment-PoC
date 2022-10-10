package com.github.davenury.ucac.consensus.ratis

import com.fasterxml.jackson.databind.JsonMappingException
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory

class HistoryStateMachine(
    val history: History,
    override var state: String = history.getCurrentEntry().getId(),
) :
    StateMachine<String>(state) {

    override fun serializeState(): String = objectMapper.writeValueAsString(state)

    override fun applyOperation(operation: String): String? =
        try {
            history.addEntry(HistoryEntry.deserialize(operation))
            state = history.getCurrentEntry().getId()
            null
        } catch (e: Exception) {
            logger.error("Error during applyOperation ${e.message}")
            e.message
        }

    override fun queryOperation(operation: String): String =
        try {
            state
        } catch (e: java.lang.IllegalArgumentException) {
            logger.error("Error during queryOperation: illegalArgumentException")
            "INVALID_OPERATION"
        }

    companion object {
        private val logger = LoggerFactory.getLogger(HistoryStateMachine::class.java)
    }
}
