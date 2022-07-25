package com.example.consensus.ratis

import com.example.common.Change
import com.example.common.History
import com.example.common.HistoryChangeResult
import com.example.common.HistoryManagement
import org.slf4j.LoggerFactory

class RatisHistoryManagement(private val historyRaftNode: HistoryRaftNode) : HistoryManagement(historyRaftNode) {
    override fun getLastChange(): ChangeWithAcceptNum? = try {
        historyRaftNode.getState()?.last()
    } catch (ex: java.util.NoSuchElementException) {
        logger.error("History is empty!")
        null
    }

    override fun change(change: Change, acceptNum: Int?): HistoryChangeResult {
        return super.change(change, acceptNum)
    }

    /**
     * Dummy implementation for simplicity of protocol. Change to correct implementation with actually building history
     * and checking if history can be built (e.g. not having conflicting changes etc.).
     * */
    override fun canBeBuild(newChange: Change): Boolean = true

    override fun getState(): History? =
        historyRaftNode.getState()

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}