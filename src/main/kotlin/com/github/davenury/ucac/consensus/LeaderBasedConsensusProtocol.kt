package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change

interface LeaderBasedConsensusProtocol: ConsensusProtocol {
    fun getLeaderId(): Int?

    suspend fun getLeaderAddress(): String?


}

data class CurrentLeaderDto(val currentLeaderPeerId: Int?)

typealias ConsensusProposeChange = Change