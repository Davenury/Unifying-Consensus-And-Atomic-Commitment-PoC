package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.ErrorMessage
import com.example.domain.HistoryManagement
import com.example.domain.RaftPeerDto
import com.example.objectMapper
import com.example.raft.RaftNode
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.apache.ratis.protocol.RaftPeer

fun Application.configureRouting(historyManagement: HistoryManagement, raftNode: RaftNode) {

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

        get("/config"){
            val peers = raftNode.getPeersGroups()
            call.respond(peers.let { objectMapper.writeValueAsString(it) })
        }

        post("/add_peer"){
            val peer = RaftPeerDto.fromJson(call.receive())
            raftNode.addPeer(peer)
            val peers = raftNode.getPeersGroups()
            call.respond(peers.let { objectMapper.writeValueAsString(it) })
        }

    }
}
