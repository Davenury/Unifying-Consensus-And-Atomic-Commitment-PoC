package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.ErrorMessage
import com.example.domain.HistoryManagement
import com.example.domain.RaftPeerDto
import com.example.objectMapper
import com.example.raft.RaftNode
import com.example.toRaftPeers
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.configureRouting(
    historyManagement: HistoryManagement,
    raftNode: RaftNode,
    peersDto: List<RaftPeerDto>
) {

    var peers: List<RaftPeerDto> = peersDto


    // Starting point for a Ktor app:
    routing {
        post("/change") {
            val change = ChangeDto(call.receive())
            val result = historyManagement.change(change.toChange())
            call.respond(result.toString())
        }
        get("/change") {
            val result = historyManagement.getLastChange()
            call.respond((result ?: ErrorMessage("Error")).let { objectMapper.writeValueAsString(it) })
        }

        get("/config") {
            call.respond(peers.let { objectMapper.writeValueAsString(it) })
        }

        post("/add_peer") {
            val peerDto = RaftPeerDto.fromJson(call.receive())
            peers = peers.plus(peerDto)
            raftNode.newPeerSet(peers.toRaftPeers())

            call.respond(peers.let { objectMapper.writeValueAsString(it) })
        }

    }
}
