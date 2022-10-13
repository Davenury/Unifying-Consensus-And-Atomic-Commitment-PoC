package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.history.History
import org.slf4j.LoggerFactory

class RatisHistoryManagement(private val historyRaftNode: HistoryRaftNode) : HistoryManagement(historyRaftNode) {
    override fun getLastChange(): Change? = try {
        historyRaftNode.getState().getCurrentEntry()
            .let { Change.fromHistoryEntry(it) }
    } catch (ex: IllegalArgumentException) {
        logger.error("History is empty!")
        null
    }

    /**
     * Dummy implementation for simplicity of protocol. Change to correct implementation with actually building history
     * and checking if history can be built (e.g. not having conflicting changes etc.).
     * */
    override fun canBeBuild(newChange: Change): Boolean = true

    override fun getState(): History =
        historyRaftNode.getState()

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}
