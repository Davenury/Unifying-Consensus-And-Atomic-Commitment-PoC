package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.meterRegistry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlin.random.Random

fun Application.commonRouting(
    gpacProtocol: GPACProtocol,
    consensusProtocol: ConsensusProtocol<Change, History>,
) {

    routing {

        post("/create_change") {
            val change = call.receive<Change>()
            gpacProtocol.performProtocolAsLeader(change)
            call.respond(HttpStatusCode.OK)
        }

        post("/consensus/create_change") {
            val change = call.receive<Change>()
            consensusProtocol.proposeChange(change)
            call.respond(HttpStatusCode.OK)
        }
        
        get("/consensus/changes") {
            call.respond(Changes(consensusProtocol.getState() ?: listOf()))
        }

        get("/consensus/change") {
            call.respond(consensusProtocol.getState()?.lastOrNull() ?: HttpStatusCode.BadRequest)
        }

    }

}
