package com.example.domain

import org.apache.ratis.protocol.RaftPeer


data class RaftPeerDto(val peerId: String, val address: String, val httpAddress: String) {
    companion object {
        fun fromJson(json: Map<String, String>): RaftPeerDto =
            RaftPeerDto(json["peerId"]!!, json["address"]!!, json["httpAddress"]!!)
    }

    fun toRaftPeer(): RaftPeer = RaftPeer.newBuilder().setId(peerId).setAddress(address).build()

    fun getPort() = address.split(":").last().toInt()
}
