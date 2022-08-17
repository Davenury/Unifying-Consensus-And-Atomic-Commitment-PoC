package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol

abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol<Change, History>) {
    open suspend fun change(change: Change, acceptNum: Int?) =
        consensusProtocol.proposeChange(change, acceptNum)
            .let {
                when (it) {
                    ConsensusFailure -> HistoryChangeResult.HistoryChangeFailure
                    ConsensusSuccess -> HistoryChangeResult.HistoryChangeSuccess
                    ConsensusResultUnknown -> HistoryChangeResult.HistoryChangeSuccess
                }
            }

    abstract fun getLastChange(): ChangeWithAcceptNum?
    abstract fun getState(): History?

    /**
     * function used to check if history can be build given another change to perform
     * */
    abstract fun canBeBuild(newChange: Change): Boolean
    abstract fun build()
}

enum class HistoryChangeResult {
    HistoryChangeSuccess, HistoryChangeFailure
}
