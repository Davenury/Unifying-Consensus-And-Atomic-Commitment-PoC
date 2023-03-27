package com.github.davenury.common

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

data class PeerId(val peerId: String) {
    override fun toString(): String = peerId
}

data class PeersetId constructor(
    val peersetId: String,
) {
    @JsonValue
    override fun toString(): String = peersetId

    // https://github.com/FasterXML/jackson-module-kotlin/issues/22
    companion object {
        @JvmStatic
        @JsonCreator
        fun create(string: String): PeersetId {
            return PeersetId(string)
        }
    }
}

data class PeerAddress(val peerId: PeerId, val address: String) {
    override fun toString(): String {
        return "($peerId at $address)"
    }

    companion object {
        fun of(peerId: String, address: String) = PeerAddress(PeerId(peerId), address)
    }
}
