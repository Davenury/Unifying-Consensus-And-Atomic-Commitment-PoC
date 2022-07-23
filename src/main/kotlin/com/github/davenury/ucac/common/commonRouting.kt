package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.commonRouting(
    gpacProtocol: GPACProtocol,
    consensusProtocol: ConsensusProtocol<Change, History>,
) {

    routing {

        post("/create_change") {
            val change = ChangeDto(call.receive())
            gpacProtocol.performProtocolAsLeader(change)
            call.respond(HttpStatusCode.OK)
        }

        post("/consensus/create_change") {

            val properties = call.receive<Map<String, Any>>()
            val change = ChangeDto(properties["change"] as Map<String, String>)
            consensusProtocol.proposeChange(change.toChange(), properties["acceptNum"] as Int?)
            call.respond(HttpStatusCode.OK)
        }

        get("/consensus/changes") {
            call.respond(consensusProtocol.getState()?.toDto() ?: listOf<ChangeWithAcceptNumDto>())
        }
        get("/consensus/change") {
            call.respond(consensusProtocol.getState()?.lastOrNull()?.toDto() ?: "")
        }
    }

}