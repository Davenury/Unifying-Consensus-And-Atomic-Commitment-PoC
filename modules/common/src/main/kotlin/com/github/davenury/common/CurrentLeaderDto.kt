package com.github.davenury.common

data class CurrentLeaderDto(val currentLeaderPeerId: PeerId?)
data class CurrentLeaderFullInfoDto(val peerId: PeerId, val peersetId: PeersetId)
data class SubscriberAddress(val address: String, val type: String)
