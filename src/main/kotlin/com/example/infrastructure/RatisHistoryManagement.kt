package com.example.infrastructure

import com.example.domain.Change
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

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}