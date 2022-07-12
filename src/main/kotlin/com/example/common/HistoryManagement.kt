package com.example.common

import com.example.consensus.ratis.ChangeWithAcceptNum
import com.example.consensus.raft.domain.ConsensusFailure
import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.consensus.raft.domain.ConsensusSuccess

abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol<Change, History>) {
    open fun change(change: Change, acceptNum: Int?) =
        consensusProtocol.proposeChange(change, acceptNum)
            .let {
                when (it) {
                    ConsensusFailure -> {
                        HistoryChangeResult.HistoryChangeFailure
                    }
                    ConsensusSuccess -> {
                        HistoryChangeResult.HistoryChangeSuccess
                    }
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

typealias History = MutableList<ChangeWithAcceptNum>
