package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import org.slf4j.LoggerFactory

class HistoryStateMachine(override var state: History) :
    StateMachine<History>(state) {

    override fun serializeState(): String = ""

    override fun applyOperation(operation: String): String? {
        return try {
            state.addEntry(HistoryEntry.deserialize(operation))
            null
        } catch (e: Exception) {
            logger.error("Error while adding entry to history", e)
            "ERROR"
        }
    }

    override fun queryOperation(operation: String): String = ""

    companion object {
        private val logger = LoggerFactory.getLogger("ratis")
    }
}
