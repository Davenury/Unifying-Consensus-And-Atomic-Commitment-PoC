package com.github.davenury.ucac.common

import com.github.davenury.common.PeersetId
import com.github.davenury.common.UnknownPeersetException
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher

/**
 * @author Kamil Jarosz
 */
class MultiplePeersetProtocols(
    config: Config,
    peerResolver: PeerResolver,
    signalPublisher: SignalPublisher,
    changeNotifier: ChangeNotifier
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
            )
        }.associateBy { it.peersetId }
    }

    fun forPeerset(peersetId: PeersetId): PeersetProtocols {
        return protocols[peersetId] ?: throw UnknownPeersetException(peersetId)
    }

    fun getHistories(): Map<PeersetId, History> {
        return protocols.mapValues { it.value.history }
    }

    fun close() {
        protocols.values.forEach { it.close() }
    }
}