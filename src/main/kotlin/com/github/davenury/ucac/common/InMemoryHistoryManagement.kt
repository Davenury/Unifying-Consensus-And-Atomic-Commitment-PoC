package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import org.slf4j.LoggerFactory

class InMemoryHistoryManagement(
    private val consensusProtocol: ConsensusProtocol<Change, History>
) : HistoryManagement(consensusProtocol) {

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override suspend fun change(change: Change): HistoryChangeResult =
        consensusProtocol.proposeChange(change)
            .let {
                when (it) {
                    ConsensusFailure -> {
                        HistoryChangeResult.HistoryChangeFailure
                    }
                    ConsensusSuccess -> {
                        HistoryChangeResult.HistoryChangeSuccess
                    }
                    ConsensusResultUnknown -> {
                        HistoryChangeResult.HistoryChangeSuccess
                    }
                }
            }

    override fun getLastChange(): Change? =
        try {
            consensusProtocol.getState()?.last()
        } catch (ex: java.util.NoSuchElementException) {
            logger.error("History is empty!")
            null
        }

    override fun getState(): History = consensusProtocol.getState() ?: mutableListOf()

    override fun canBeBuild(newChange: Change): Boolean = true

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryHistoryManagement::class.java)
    }
}
