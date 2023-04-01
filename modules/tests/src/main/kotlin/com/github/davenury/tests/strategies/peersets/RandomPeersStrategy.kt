package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.PeersetId
import com.github.davenury.common.Notification


class RandomPeersStrategy(
    private val peersets: List<PeersetId>
): GetPeersStrategy {
    override suspend fun getPeersets(numberOfPeersets: Int, changeId: String): List<PeersetId> =
        peersets.shuffled().take(numberOfPeersets)

    override suspend fun handleNotification(notification: Notification) {}
    override suspend fun freePeersets(peersetsId: List<PeersetId>, changeId: String) {}
}
