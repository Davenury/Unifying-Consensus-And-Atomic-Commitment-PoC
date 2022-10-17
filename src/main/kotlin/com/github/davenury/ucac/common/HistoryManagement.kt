package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.history.History

abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol) {
    open suspend fun change(change: Change) =
        consensusProtocol.proposeChange(change)
            .let {
                when (it) {
                    ConsensusFailure -> HistoryChangeResult.HistoryChangeFailure
                    ConsensusSuccess -> HistoryChangeResult.HistoryChangeSuccess
                    ConsensusResultUnknown -> HistoryChangeResult.HistoryChangeUnknown
                    ConsensusChangeAlreadyProposed -> HistoryChangeResult.HistoryChangeUnknown
                }
            }

    fun contains(changeId: String): Boolean = getState().getEntryFromHistory(changeId) != null

    abstract fun getLastChange(): Change?
    abstract fun getState(): History

    /**
     * function used to check if history can be build given another change to perform
     * */
    abstract fun canBeBuild(newChange: Change): Boolean
    abstract fun build()
}

enum class HistoryChangeResult {
    HistoryChangeSuccess, HistoryChangeFailure, HistoryChangeUnknown
}
