package com.example.consensus.raft.infrastructure

import com.example.common.Change
import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.consensus.raft.domain.ConsensusResult
import com.example.consensus.raft.domain.ConsensusSuccess

class DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<Change>> {
    private val historyStorage: MutableList<Change> = mutableListOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        historyStorage.add(change)
        return ConsensusSuccess
    }

    override fun getState(): MutableList<Change> = historyStorage
}