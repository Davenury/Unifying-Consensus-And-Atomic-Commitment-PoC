package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol: ConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogId: String): ConsensusElectedYou
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    fun setPeerAddress(address: String)
    suspend fun getLeaderAddress(): String?
    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    fun stop()
}
