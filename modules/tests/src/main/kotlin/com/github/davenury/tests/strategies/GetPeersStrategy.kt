package com.github.davenury.tests.strategies

interface GetPeersStrategy {
    suspend fun getPeersets(numberOfPeersets: Int): List<Int>
    suspend fun freePeersets(peersetsId: List<Int>)
    suspend fun handleNotification(peersetId: Int)
}