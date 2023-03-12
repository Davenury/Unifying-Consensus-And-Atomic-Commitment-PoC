package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.Notification


class RandomPeersStrategy(
    private val peersetsRange: IntRange
): GetPeersStrategy {
    override suspend fun getPeersets(numberOfPeersets: Int): List<Int> =
        peersetsRange.shuffled().take(numberOfPeersets)

    override suspend fun handleNotification(notification: Notification) {}
    override suspend fun freePeersets(peersetsId: List<Int>) {}
    override fun setCurrentChange(changeId: String) {}
}
