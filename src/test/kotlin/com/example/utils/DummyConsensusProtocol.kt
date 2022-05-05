package com.example.utils

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.ConsensusResult
import com.example.domain.ConsensusSuccess

object DummyConsensusProtocol: ConsensusProtocol<Change, MutableList<Change>> {
    private var response: ConsensusResult = ConsensusSuccess

    override fun proposeChange(change: Change): ConsensusResult
            = response

    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<Change>? = mutableListOf()
}