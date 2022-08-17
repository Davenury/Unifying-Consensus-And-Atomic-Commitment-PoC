package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*


object DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<ChangeWithAcceptNum>> {
    private var response: ConsensusResult = ConsensusSuccess

    override suspend fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<ChangeWithAcceptNum> = mutableListOf()
}