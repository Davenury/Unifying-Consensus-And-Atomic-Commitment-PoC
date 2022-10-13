package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.history.History

class DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private val historyStorage: History = History()

    override suspend fun proposeChange(change: Change): ConsensusResult {
        historyStorage.addEntry(change.toHistoryEntry())
        return ConsensusSuccess
    }

    override fun getState(): History = historyStorage
}
