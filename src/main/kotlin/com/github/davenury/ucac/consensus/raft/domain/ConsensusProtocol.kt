package com.github.davenury.ucac.consensus.raft.domain

interface ConsensusProtocol<A, B> {
    suspend fun proposeChange(change: A, acceptNum: Int?): ConsensusResult

    fun getState(): B?
}


enum class ConsensusResult {
    ConsensusSuccess, ConsensusFailure, ConsensusResultUnknown
}

