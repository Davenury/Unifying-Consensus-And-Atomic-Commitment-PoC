package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.history.History


object DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private var response: ConsensusResult = ConsensusSuccess
    var change: Change? = null

    override suspend fun proposeChange(change: Change): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): History = change?.let { listOf(it) }
        ?.let { Changes(it) }
        ?.toHistory()
        ?: History()
}
