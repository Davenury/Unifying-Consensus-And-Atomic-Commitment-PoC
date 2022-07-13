package com.example.utils

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.ConsensusResult
import com.example.domain.ConsensusSuccess
import com.example.ratis.ChangeWithAcceptNum

object DummyConsensusProtocol: ConsensusProtocol<Change, MutableList<ChangeWithAcceptNum>> {
    private var response: ConsensusResult = ConsensusSuccess

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult
            = response



    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<ChangeWithAcceptNum> = mutableListOf()
}