package com.example.consensus.raft.domain

import com.example.common.ChangeWithAcceptNum
import com.example.common.ChangeWithAcceptNumDto
import com.example.common.History

interface RaftConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(peerId: Int, iteration: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, iteration: Int)
    suspend fun handleHeartbeat(
        peerId: Int,
        acceptedChanges: List<LedgerItem>,
        proposedChanges: List<LedgerItem>
    )

    suspend fun handleProposeChange(change: ChangeWithAcceptNum)
    fun getLeaderAddress(): String?
    fun getProposedChanges(): History
    fun getAcceptedChanges(): History
}
