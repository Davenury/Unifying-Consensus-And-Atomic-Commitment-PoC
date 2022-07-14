package com.example.consensus.raft.infrastructure

import com.example.common.Change
import com.example.common.History
import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.consensus.raft.domain.ConsensusResult
import com.example.consensus.raft.domain.ConsensusSuccess
import com.example.consensus.ratis.ChangeWithAcceptNum

class DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private val historyStorage: History = mutableListOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
        return ConsensusSuccess
    }

    override fun getState(): History = historyStorage
}