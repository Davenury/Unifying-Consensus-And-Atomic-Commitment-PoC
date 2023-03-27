package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.PeersetId

interface GetPeersStrategy {
    suspend fun getPeersets(numberOfPeersets: Int): List<PeersetId>
    suspend fun freePeersets(peersetsId: List<PeersetId>)
    suspend fun handleNotification(peersetId: PeersetId)
}
