package com.github.davenury.ucac.common

/**
 * @author Kamil Jarosz
 */
class PeerResolver(
    private val currentPeer: GlobalPeerId,
    private var peers: List<List<String>>,
) {
    fun resolve(globalPeerId: GlobalPeerId): PeerAddress {
        return PeerAddress(globalPeerId, peers[globalPeerId.peersetId][globalPeerId.peerId])
    }

    fun currentPeerAddress() = resolve(currentPeer)

    fun getPeersFromCurrentPeerset(): List<PeerAddress> {
        return getPeersFromPeerset(currentPeer.peersetId)
    }

    fun getPeersFromPeerset(peersetId: Int): List<PeerAddress> {
        return peers[peersetId].mapIndexed { index, address ->
            PeerAddress(GlobalPeerId(peersetId, index), address)
        }
    }

    fun getPeers() = peers

    fun setPeers(newPeers: List<List<String>>) {
        peers = newPeers.map { ArrayList(it) }
    }

    fun findPeersetWithPeer(peer: String): Int? {
        peers.forEachIndexed { index, peerset ->
            if (peerset.contains(peer)) {
                return index
            }
        }
        return null
    }
}
