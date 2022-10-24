package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History
import java.util.concurrent.CompletableFuture


object DummyConsensusProtocol : ConsensusProtocol {
    private var response: ConsensusResult = ConsensusSuccess
    private var responseAsync: ChangeResult = ChangeResult(ChangeResult.Status.SUCCESS)
    var change: Change? = null

    @Deprecated("use proposeChangeAsync")
    override suspend fun proposeChange(change: Change): ConsensusResult = response

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> =
        CompletableFuture.completedFuture(responseAsync)

    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    fun setResponseAsync(response: ChangeResult) {
        this.responseAsync = response
    }

    override fun getState(): History {
        val h = History()
        if (change != null) {
            h.addEntry(change!!.toHistoryEntry())
        }
        return h
    }
}
