package com.github.davenury.checker

import com.github.davenury.common.*

data class Config(
    // peer1=X;peer2=Y
    val peers: String,
    // peerset1=peer1,peer2;peerset2=peer3,peer4
    val peersets: String,

    val numberOfThreads: Int = 1,
) {

    private val parsedPeersets: Map<PeersetId, List<PeerId>> = parsePeersets(this.peersets)
    private val parsedPeers: Map<PeerId, PeerAddress> = parsePeers(peers)

    fun getPeersets(): Map<PeersetId, List<PeerAddress>> =
        parsedPeersets.map { (peersetId, peers) ->
            peersetId to peers.map { getPeer(it)!! }
        }.toMap()

    fun getPeers(): Map<PeerId, PeerAddress> = parsedPeers
    fun getPeer(peerId: PeerId) = parsedPeers[peerId]
    fun getPeerset(peersetId: PeersetId) = parsedPeersets[peersetId]
}