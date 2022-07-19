package com.example.consensus.raft.domain

import com.example.common.ChangeWithAcceptNum
import com.example.common.ChangeWithAcceptNumDto

interface RaftConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(peerId: Int, iteration: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, iteration: Int)
    suspend fun handleHeartbeat(
        peerId: Int,
        acceptedChanges: List<ChangeWithAcceptNum>,
        proposedChanges: List<ChangeWithAcceptNum>
    )

    suspend fun handleProposeChange(change: ChangeWithAcceptNum)
}
