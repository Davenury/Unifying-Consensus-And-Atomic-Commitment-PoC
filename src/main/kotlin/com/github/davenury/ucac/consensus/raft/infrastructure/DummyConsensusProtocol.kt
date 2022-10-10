package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry

class DummyConsensusProtocol(private val history: History) : ConsensusProtocol {
    override suspend fun proposeChange(entry: HistoryEntry): ConsensusResult {
        history.addEntry(entry)
        return ConsensusSuccess
    }

    override fun getState(): History = history
}
