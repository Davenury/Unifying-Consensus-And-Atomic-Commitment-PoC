package com.example.infrastructure

import com.example.domain.*

class InMemoryHistoryManagement(
    consensusProtocol: ConsensusProtocol
): HistoryManagement(consensusProtocol) {
    private val historyStorage: MutableList<Change> = mutableListOf()

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override fun change(change: Change) =
        consensusProtocol.proposeChange(change)
            .let { when(it) {
                ConsensusFailure -> {
                    HistoryChangeFailure
                }
                ConsensusSuccess -> {
                    historyStorage.add(change)
                    HistoryChangeSuccess
                }
            } }

    override fun getLastChange(): Change? =
        try {
            historyStorage.last()
        } catch (ex: java.util.NoSuchElementException) {
            null
        }

}