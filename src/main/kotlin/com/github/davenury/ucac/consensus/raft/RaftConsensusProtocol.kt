package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerId
import com.github.davenury.ucac.consensus.ConsensusProtocol
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol: ConsensusProtocol {
    suspend fun handleRequestVote(peerId: PeerId, iteration: Int, lastLogId: String): ConsensusElectedYou
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
}
