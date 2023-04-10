package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId

interface Subscriber {
    val type: String
    suspend fun notifyConsensusLeaderChange(newLeaderPeerId: PeerId, newLeaderPeersetId: PeersetId)

    companion object {
        fun of(address: String, type: String) =
            when (type) {
                "http" -> HttpSubscriber(address)
                else -> throw UnsupportedOperationException("Subscriber of type: $type is not supported")
            }
    }
}
