package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import kotlinx.coroutines.runBlocking

object Subscribers {
    private val subscribers = mutableListOf<Subscriber>()

    fun registerSubscriber(subscriber: Subscriber) {
        subscribers.add(subscriber)
    }

    fun notifyAboutConsensusLeaderChange(newPeerId: PeerId, newPeersetId: PeersetId) {
        runBlocking {
            subscribers.forEach { it.notifyConsensusLeaderChange(newPeerId, newPeersetId) }
        }
    }
}