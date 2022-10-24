package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    @Deprecated("use proposeChangeAsync")
    suspend fun proposeChange(change: Change): ConsensusResult

    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    fun getState(): History
}


enum class ConsensusResult {
    ConsensusSuccess,
    ConsensusFailure,
    ConsensusResultUnknown,
    ConsensusChangeAlreadyProposed,
}

enum class RaftRole {
    Leader,
    Follower,
    Candidate,
}

