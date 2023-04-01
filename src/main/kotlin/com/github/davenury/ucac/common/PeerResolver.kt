package com.github.davenury.ucac.common

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
/**
 * @author Kamil Jarosz
 */
class PeerResolver(
    private val currentPeer: PeerId,
    peers: Map<PeerId, PeerAddress>,
    peersets: Map<PeersetId, List<PeerId>>,
) {
    private val peers: MutableMap<PeerId, PeerAddress>
    private val peersets: MutableMap<PeersetId, MutableList<PeerId>>

    init {
        this.peers = HashMap(peers)
        this.peersets = HashMap(peersets.mapValues { ArrayList(it.value) })
    }

    fun resolve(peerId: String): PeerAddress {
        return resolve(PeerId(peerId))
    }

    fun resolve(peerId: PeerId): PeerAddress {
        return peers[peerId]!!
    }

    fun currentPeer(): PeerId = currentPeer

    fun currentPeerAddress(): PeerAddress = resolve(currentPeer)

    fun getPeersFromPeerset(peersetId: PeersetId): List<PeerAddress> {
        return (peersets[peersetId] ?: listOf())
            .sortedBy { it.peerId }
            .map { resolve(it) }
    }

    fun setPeerAddress(peerId: PeerId, address: PeerAddress) {
        peers[peerId] = address
    }

    fun addPeerToPeerset(peersetId: PeersetId, peerId: PeerId) {
        peersets[peersetId]!!.add(peerId)
    }
}
