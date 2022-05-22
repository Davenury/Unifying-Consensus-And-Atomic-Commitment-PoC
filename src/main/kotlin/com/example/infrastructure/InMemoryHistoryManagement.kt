package com.example.infrastructure

import com.example.domain.*
import com.example.raft.History
import org.slf4j.LoggerFactory

class InMemoryHistoryManagement(
    private val consensusProtocol: ConsensusProtocol<Change, History>
) : HistoryManagement(consensusProtocol) {
    private val historyStorage: MutableList<Change> = mutableListOf()

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override fun change(change: Change) =
        consensusProtocol.proposeChange(change)
            .let {
                when (it) {
                    ConsensusFailure -> {
                        HistoryChangeResult.HistoryChangeFailure
                    }
                    ConsensusSuccess -> {
                        historyStorage.add(change)
                        HistoryChangeResult.HistoryChangeSuccess
                    }
                }
            }

    override fun getLastChange(): Change? =
        try {
            historyStorage.last()
        } catch (ex: java.util.NoSuchElementException) {
            logger.error("History is empty!")
            null
        }

    override fun canBeBuild(newChange: Change): Boolean
        = true

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryHistoryManagement::class.java)
    }
}