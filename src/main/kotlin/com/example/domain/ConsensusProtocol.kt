package com.example.domain

interface ConsensusProtocol {
    fun proposeChange(change: Change): ConsensusResult
}

sealed class ConsensusResult
object ConsensusSuccess: ConsensusResult()
object ConsensusFailure: ConsensusResult()