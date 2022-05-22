package com.example.infrastructure

import com.example.domain.Change
import com.example.domain.HistoryChangeResult
import com.example.domain.HistoryManagement
import com.example.raft.HistoryRaftNode
import org.slf4j.LoggerFactory

class RatisHistoryManagement(private val historyRaftNode: HistoryRaftNode) : HistoryManagement(historyRaftNode) {
    override fun getLastChange(): Change? = try {
        historyRaftNode.getState()?.last()
    } catch (ex: java.util.NoSuchElementException) {
        logger.error("History is empty!")
        null
    }

    override fun change(change: Change): HistoryChangeResult {
        return super.change(change)
    }

    /**
     * Dummy implementation for simplicity of protocol. Change to correct implementation with actually building history
     * and checking if history can be built (e.g. not having conflicting changes etc.).
     * */
    override fun canBeBuild(newChange: Change): Boolean
        = true

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}