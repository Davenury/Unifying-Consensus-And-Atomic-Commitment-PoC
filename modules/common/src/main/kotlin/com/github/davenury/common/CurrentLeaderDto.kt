package com.github.davenury.common
data class CurrentLeaderFullInfoDto(val peerId: PeerId, val peersetId: PeersetId)
data class SubscriberAddress(val address: String, val type: String)

data class PeersetInformation(
    val currentConsensusLeader: PeerId?,
    val peersInPeerset: List<PeerId>
)
