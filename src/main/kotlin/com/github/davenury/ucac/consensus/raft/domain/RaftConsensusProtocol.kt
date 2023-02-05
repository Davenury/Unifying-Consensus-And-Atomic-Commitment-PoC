package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogId: String): ConsensusElectedYou
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
    fun setPeerAddress(address: String)
    suspend fun getLeaderAddress(): String?
    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    fun stop()
}
