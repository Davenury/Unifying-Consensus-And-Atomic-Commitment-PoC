package com.example.infrastructure

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.ConsensusResult
import com.example.domain.ConsensusSuccess

class DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<Change>> {
    private val historyStorage: MutableList<Change> = mutableListOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        historyStorage.add(change)
        return ConsensusSuccess
    }

    override fun getState(): MutableList<Change> = historyStorage
}