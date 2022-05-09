package com.example.domain

import org.apache.ratis.protocol.RaftPeer

object RaftPeerDto {
    fun fromJson(json: Map<String, String>): RaftPeer =
        RaftPeer.newBuilder().setId(json["peerId"]).setAddress(json["address"]).build()
}