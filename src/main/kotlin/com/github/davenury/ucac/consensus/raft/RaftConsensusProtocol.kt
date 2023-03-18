package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.consensus.LeaderBasedConsensusProtocol
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol: LeaderBasedConsensusProtocol {
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogId: String): ConsensusElectedYou
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
}
