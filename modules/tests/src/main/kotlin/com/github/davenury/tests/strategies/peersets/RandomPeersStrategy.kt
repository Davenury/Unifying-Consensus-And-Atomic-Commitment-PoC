package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.PeersetId


class RandomPeersStrategy(
    private val peersets: List<PeersetId>
): GetPeersStrategy {
    override suspend fun getPeersets(numberOfPeersets: Int): List<PeersetId> =
        peersets.shuffled().take(numberOfPeersets)

    override suspend fun handleNotification(peersetId: PeersetId) {}
    override suspend fun freePeersets(peersetsId: List<PeersetId>) {}
}
