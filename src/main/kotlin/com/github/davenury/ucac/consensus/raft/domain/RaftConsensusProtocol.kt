package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import java.util.concurrent.CompletableFuture

interface RaftConsensusProtocol {
    suspend fun begin()
    fun setOtherPeers(otherPeers: List<String>)
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogIndex: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int)
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
    fun setPeerAddress(address: String)
    suspend fun getLeaderAddress(): String?
    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    fun stop()
}
