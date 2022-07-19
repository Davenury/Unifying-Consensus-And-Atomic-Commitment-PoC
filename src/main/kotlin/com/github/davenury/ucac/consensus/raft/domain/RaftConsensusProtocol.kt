package com.github.davenury.ucac.consensus.raft.domain

interface RaftConsensusProtocol {
    suspend fun begin()
    fun setOtherPeers(otherPeers: List<String>)
    suspend fun handleRequestVote(peerId: Int, iteration: Int): ConsensusElectedYou
    suspend fun handleLeaderElected(peerId: Int, peerAddress: String, iteration: Int)
    suspend fun handleHeartbeat(
        peerId: Int,
        acceptedChanges: List<ChangeWithAcceptNum>,
        proposedChanges: List<ChangeWithAcceptNum>
    )

    suspend fun handleProposeChange(change: ChangeWithAcceptNum)
}