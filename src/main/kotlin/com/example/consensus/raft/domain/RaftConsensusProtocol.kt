package com.example.consensus.raft.domain

interface RaftConsensusProtocol {
    suspend fun begin()
    suspend fun handleRequestVote(message: ConsensusElectMe): ConsensusElectedYou
    suspend fun handleLeaderElected(message: ConsensusImTheLeader)
}