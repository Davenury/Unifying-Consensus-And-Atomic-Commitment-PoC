package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History


object DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private var response: ConsensusResult = ConsensusSuccess
    var change: Change? = null

    override suspend fun proposeChange(change: Change): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): History {
        val h = History()
        if (change != null) {
            h.addEntry(change!!.toHistoryEntry())
        }
        return h
    }
}
