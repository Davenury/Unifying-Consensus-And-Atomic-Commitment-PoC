package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId

interface Subscriber {
    suspend fun notifyConsensusLeaderChange(newLeaderPeerId: PeerId, newLeaderPeersetId: PeersetId)

}
