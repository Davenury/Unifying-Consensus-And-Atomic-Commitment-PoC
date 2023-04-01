package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.Notification
import com.github.davenury.common.PeersetId

interface GetPeersStrategy {
    suspend fun getPeersets(numberOfPeersets: Int, changeId: String): List<PeersetId>
    suspend fun freePeersets(peersetsId: List<PeersetId>, changeId: String)
    suspend fun handleNotification(notification: Notification)
}
