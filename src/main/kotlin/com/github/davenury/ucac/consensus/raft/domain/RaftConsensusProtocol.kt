package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerId
import com.github.davenury.ucac.consensus.LeaderBasedConsensusProtocol
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol: LeaderBasedConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(peerId: PeerId, iteration: Int, lastLogId: String): ConsensusElectedYou
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    fun stop()
}
