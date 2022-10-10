package com.github.davenury.ucac.utils

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry


class DummyConsensusProtocol : ConsensusProtocol {
    private var response: ConsensusResult = ConsensusSuccess
    var history: History = History()

    override suspend fun proposeChange(entry: HistoryEntry): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): History = history
}
