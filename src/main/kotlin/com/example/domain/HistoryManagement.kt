package com.example.domain

abstract class HistoryManagement(
    protected val consensusProtocol: ConsensusProtocol
) {
    abstract fun change(change: Change): HistoryChangeResult

    abstract fun getLastChange(): Change?
}

sealed class HistoryChangeResult
object HistoryChangeSuccess: HistoryChangeResult()
object HistoryChangeFailure: HistoryChangeResult()
