package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId

class CodeSubscriber(
    val action: (PeerId, PeersetId) -> Unit
): Subscriber {

    override suspend fun notifyConsensusLeaderChange(newLeaderPeerId: PeerId, newLeaderPeersetId: PeersetId) {
        action(newLeaderPeerId, newLeaderPeersetId)
    }
}