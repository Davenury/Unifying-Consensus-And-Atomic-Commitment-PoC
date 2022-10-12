package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*


object DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<Change>> {
    private var response: ConsensusResult = ConsensusSuccess
    var change: Change? = null

    override suspend fun proposeChange(change: Change): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<Change> = mutableListOf(change).filterNotNull().toMutableList()
}
