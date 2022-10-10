package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.history.HistoryEntry
import org.slf4j.LoggerFactory
import com.github.davenury.ucac.history.History

class InMemoryHistoryManagement(
    private val consensusProtocol: ConsensusProtocol,
    history: History,
) : HistoryManagement(consensusProtocol, history) {

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override suspend fun change(entry: HistoryEntry): HistoryChangeResult =
        consensusProtocol.proposeChange(entry)
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

    override fun getLastChange(): HistoryEntry = history.getCurrentEntry()

    override fun getState(): History = history

    override fun canBeBuild(newChange: Change): Boolean = true

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryHistoryManagement::class.java)
    }
}
