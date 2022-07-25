package com.example.utils

import com.example.common.Change
import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.consensus.raft.domain.ConsensusResult
import com.example.consensus.raft.domain.ConsensusSuccess
import com.example.consensus.ratis.ChangeWithAcceptNum

object DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<ChangeWithAcceptNum>> {
    private var response: ConsensusResult = ConsensusSuccess

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<ChangeWithAcceptNum> = mutableListOf()
}