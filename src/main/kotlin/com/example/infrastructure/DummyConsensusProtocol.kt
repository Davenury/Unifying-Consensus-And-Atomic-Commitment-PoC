package com.example.infrastructure

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.ConsensusResult
import com.example.domain.ConsensusSuccess

object DummyConsensusProtocol: ConsensusProtocol {
    override fun proposeChange(change: Change): ConsensusResult
        = ConsensusSuccess
}