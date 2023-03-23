package com.github.davenury.ucac.consensus

import com.github.davenury.common.PeerId

interface LeaderBasedConsensusProtocol: ConsensusProtocol {
    fun getLeaderId(): PeerId?
}
