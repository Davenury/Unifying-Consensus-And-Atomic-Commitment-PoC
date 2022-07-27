package com.github.davenury.ucac.consensus.raft.domain

interface RaftConsensusProtocol {
    suspend fun begin()
    fun setOtherPeers(otherPeers: List<String>)
    suspend fun handleRequestVote(message: ConsensusElectMe): ConsensusElectedYou
    suspend fun handleLeaderElected(message: ConsensusImTheLeader)
}