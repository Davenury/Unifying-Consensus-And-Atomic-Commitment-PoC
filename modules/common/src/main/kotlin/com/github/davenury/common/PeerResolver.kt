package com.github.davenury.common

/**
 * @author Kamil Jarosz
 */
class PeerResolver(
    private val currentPeer: GlobalPeerId,
    peers: Map<GlobalPeerId, PeerAddress>,
) {
    private val peers: MutableMap<GlobalPeerId, PeerAddress> = HashMap()

    init {
        this.peers.putAll(peers)
    }

    fun resolve(globalPeerId: GlobalPeerId): PeerAddress {
        return peers[globalPeerId]!!
    }

    fun currentPeer(): GlobalPeerId = currentPeer

    fun currentPeerAddress(): PeerAddress = resolve(currentPeer)

    fun getPeersFromCurrentPeerset(): List<PeerAddress> {
        return getPeersFromPeerset(currentPeer.peersetId)
    }

    fun getPeersFromPeerset(peersetId: Int): List<PeerAddress> {
        return peers.values
            .filter { it.globalPeerId.peersetId == peersetId }
            .sortedBy { it.globalPeerId.peerId }
    }

    fun getPeersPrintable(): List<List<String>> = peers.asSequence()
        .groupBy { it.key.peersetId }
        .map { it.value }
        .map { peerset -> peerset.map { it.value.address } }

    fun setPeers(newPeers: Map<GlobalPeerId, PeerAddress>) {
        peers.putAll(newPeers)
    }
}
