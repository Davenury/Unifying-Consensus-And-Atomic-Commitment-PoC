package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class Subscribers {
    private val subscribers = mutableListOf<Subscriber>()

    fun registerSubscriber(subscriber: Subscriber) {
        subscribers.add(subscriber)
    }

    fun notifyAboutConsensusLeaderChange(newPeerId: PeerId, newPeersetId: PeersetId) {
        GlobalScope.launch {
            subscribers.forEach { 
                launch {
                    it.notifyConsensusLeaderChange(newPeerId, newPeersetId)
                }
            }
        }
    }
}