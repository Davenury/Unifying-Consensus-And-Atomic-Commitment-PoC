package com.github.davenury.ucac.common

import com.github.davenury.common.PeersetId
import com.github.davenury.common.UnknownPeersetException
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.structure.Subscriber
import com.github.davenury.ucac.common.structure.Subscribers

/**
 * @author Kamil Jarosz
 */
class MultiplePeersetProtocols(
    config: Config,
    peerResolver: PeerResolver,
    signalPublisher: SignalPublisher,
    changeNotifier: ChangeNotifier,
    private val subscribers: Map<PeersetId, Subscribers>
) {
    val protocols: Map<PeersetId, PeersetProtocols>

    init {
        protocols = config.peersetIds().map { peersetId ->
            PeersetProtocols(
                peersetId,
                config,
                peerResolver,
                signalPublisher,
                changeNotifier,
                subscribers[peersetId]
            )
        }.associateBy { it.peersetId }
    }

    fun forPeerset(peersetId: PeersetId): PeersetProtocols {
        return protocols[peersetId] ?: throw UnknownPeersetException(peersetId)
    }

    fun registerSubscriber(peersetId: PeersetId, subscriber: Subscriber) {
        subscribers[peersetId]?.registerSubscriber(subscriber)
    }

    fun close() {
        protocols.values.forEach { it.close() }
    }
}
