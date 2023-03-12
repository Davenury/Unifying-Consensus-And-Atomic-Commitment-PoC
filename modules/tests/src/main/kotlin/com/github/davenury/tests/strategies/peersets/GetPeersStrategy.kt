package com.github.davenury.tests.strategies.peersets

import com.github.davenury.common.Notification

interface GetPeersStrategy {
    suspend fun getPeersets(numberOfPeersets: Int): List<Int>
    suspend fun freePeersets(peersetsId: List<Int>)
    suspend fun handleNotification(notification: Notification)
    fun setCurrentChange(changeId: String)
}
