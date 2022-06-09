package com.example.domain

interface ConsensusProtocol<A, B> {
    fun proposeChange(change: A, acceptNum: Int? = null): ConsensusResult

    fun getState(): B?
}

sealed class ConsensusResult
object ConsensusSuccess : ConsensusResult()
object ConsensusFailure : ConsensusResult()