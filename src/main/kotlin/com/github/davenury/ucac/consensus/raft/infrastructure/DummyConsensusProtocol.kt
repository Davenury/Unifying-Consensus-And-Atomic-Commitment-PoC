package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.history.History
import java.util.concurrent.CompletableFuture

class DummyConsensusProtocol : ConsensusProtocol {
    private val historyStorage: History = History()

    @Deprecated("use proposeChangeAsync")
    override suspend fun proposeChange(change: Change): ConsensusResult {
        historyStorage.addEntry(change.toHistoryEntry())
        return ConsensusSuccess
    }

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        historyStorage.addEntry(change.toHistoryEntry())
        return CompletableFuture.completedFuture(ChangeResult(ChangeResult.Status.SUCCESS))
    }

    override fun getState(): History = historyStorage
}
