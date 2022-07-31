package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum

class DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private val historyStorage: History = mutableListOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
        return ConsensusSuccess
    }

    override fun getState(): History = historyStorage
}