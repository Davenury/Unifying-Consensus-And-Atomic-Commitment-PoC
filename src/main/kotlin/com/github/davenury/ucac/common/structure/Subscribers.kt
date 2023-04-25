package com.github.davenury.ucac.common.structure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import kotlinx.coroutines.*
import java.util.concurrent.Executors

class Subscribers {
    private val subscribers = mutableListOf<Subscriber>()

    fun registerSubscriber(subscriber: Subscriber) {
        subscribers.add(subscriber)
    }

    fun notifyAboutConsensusLeaderChange(newPeerId: PeerId, newPeersetId: PeersetId) {
        coroutineScope.launch {
            subscribers.forEach {
                launch {
                    it.notifyConsensusLeaderChange(newPeerId, newPeersetId)
                }
            }
        }
    }

    companion object {
        private val context = Executors.newCachedThreadPool()
        private val coroutineScope = CoroutineScope(context.asCoroutineDispatcher())
    }
}