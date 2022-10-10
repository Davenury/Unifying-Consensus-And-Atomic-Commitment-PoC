package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry

interface ConsensusProtocol {
    suspend fun proposeChange(entry: HistoryEntry): ConsensusResult

    fun getState(): History
}


enum class ConsensusResult {
    ConsensusSuccess,
    ConsensusFailure,
    ConsensusResultUnknown,
}
