package com.example.api

import com.example.NodeIdAndPortOffset
import com.example.domain.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.protocolRouting(protocol: GPACProtocol, allPeers: List<List<String>>) {

    routing {
        post("/create_change") {
            val change = ChangeDto(call.receive())
            protocol.performProtocolAsLeader(change, allPeers)
            call.respond(HttpStatusCode.OK)
        }

        post("/elect") {
            val message = call.receive<ElectMe>()
            call.respond(protocol.handleElect(message))
        }

        post("/ft-agree") {
            val message = call.receive<Agree>()
            call.respond(protocol.handleAgree(message))
        }

        post("/apply") {
            val message = call.receive<Apply>()
            protocol.handleApply(message)
            call.respond(HttpStatusCode.OK)
        }
    }

}