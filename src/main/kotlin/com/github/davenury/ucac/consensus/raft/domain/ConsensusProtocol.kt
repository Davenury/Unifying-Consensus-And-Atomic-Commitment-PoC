package com.github.davenury.ucac.consensus.raft.domain

interface ConsensusProtocol<A, B> {
    suspend fun proposeChange(change: A): ConsensusResult

    fun getState(): B?
}


enum class ConsensusResult {
    ConsensusSuccess, ConsensusFailure, ConsensusResultUnknown
}

