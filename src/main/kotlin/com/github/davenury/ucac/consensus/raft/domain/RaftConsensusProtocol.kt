package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.ChangeWithAcceptNum
import com.github.davenury.ucac.common.History

interface RaftConsensusProtocol {
    suspend fun begin()
    fun setOtherPeers(otherPeers: List<String>)
    suspend fun handleRequestVote(peerId: Int, iteration: Int, lastAcceptedId: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, iteration: Int)
    suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): Boolean

    suspend fun handleProposeChange(change: ChangeWithAcceptNum): ConsensusResult
    fun setLeaderAddress(address: String)
    fun getLeaderAddress(): String?
    fun getProposedChanges(): History
    fun getAcceptedChanges(): History
}