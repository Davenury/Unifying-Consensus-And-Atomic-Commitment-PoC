package com.example.domain

import com.example.raft.History


abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol<Change, History>) {
    open fun change(change: Change) =
        consensusProtocol.proposeChange(change)
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

    abstract fun getLastChange(): Change?
}

enum class HistoryChangeResult {
    HistoryChangeSuccess, HistoryChangeFailure

}
