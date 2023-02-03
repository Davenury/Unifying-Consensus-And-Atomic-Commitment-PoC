package com.github.davenury.tests.strategies.peersets


class RandomPeersStrategy(
    private val peersetsRange: IntRange
): GetPeersStrategy {
    override suspend fun getPeersets(numberOfPeersets: Int): List<Int> =
        peersetsRange.shuffled().take(numberOfPeersets)

    override suspend fun handleNotification(peersetId: Int) {}
    override suspend fun freePeersets(peersetsId: List<Int>) {}
}