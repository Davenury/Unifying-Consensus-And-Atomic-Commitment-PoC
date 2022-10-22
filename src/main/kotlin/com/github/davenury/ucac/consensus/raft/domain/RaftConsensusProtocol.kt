package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change

interface RaftConsensusProtocol {
    suspend fun begin()
    fun setOtherPeers(otherPeers: List<String>)
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogIndex: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int)
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse
    suspend fun handleAsyncProposeChange(change: Change): String
    suspend fun handleSyncProposeChange(change: Change): ConsensusResult
    fun setPeerAddress(address: String)
    suspend fun getLeaderAddress(): String?
    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    fun stop()
}
