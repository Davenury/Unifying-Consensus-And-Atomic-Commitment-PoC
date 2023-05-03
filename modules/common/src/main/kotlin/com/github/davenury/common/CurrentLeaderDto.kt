package com.github.davenury.common
data class CurrentLeaderFullInfoDto(val peerId: PeerId, val peersetId: PeersetId)
data class SubscriberAddress(val address: String, val type: String)

data class PeersetInformation(
    val currentConsensusLeader: PeerId?,
    val peersInPeerset: List<PeerId>
) {
    fun toDto() = PeersetInformationDto(
        currentConsensusLeaderId = this.currentConsensusLeader?.peerId,
        peersInPeerset = this.peersInPeerset.map { it.peerId }
    )
}

data class PeersetInformationDto(
    val currentConsensusLeaderId: String?,
    val peersInPeerset: List<String>
) {
    fun toDomain() = PeersetInformation(
        currentConsensusLeader = this.currentConsensusLeaderId?.let { PeerId(it) },
        peersInPeerset = this.peersInPeerset.map { PeerId(it) }
    )
}
