package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*

class DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private val historyStorage: History = mutableListOf()

    override suspend fun proposeChange(change: Change): ConsensusResult {
        historyStorage.add(change)
        return ConsensusSuccess
    }

    override fun getState(): History = historyStorage
}
