package com.example.domain

import com.example.raft.History


abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol<Change, History>) {
    open fun change(change: Change) =
        consensusProtocol.proposeChange(change)
            .let {
                when (it) {
                    ConsensusFailure -> {
                        HistoryChangeFailure
                    }
                    ConsensusSuccess -> {
                        HistoryChangeSuccess
                    }
                }
            }

    abstract fun getLastChange(): Change?
}

sealed class HistoryChangeResult
object HistoryChangeSuccess : HistoryChangeResult()
object HistoryChangeFailure : HistoryChangeResult()
