package com.github.davenury.ucac.consensus

interface LeaderBasedConsensusProtocol: ConsensusProtocol {
    fun getLeaderId(): Int?
}