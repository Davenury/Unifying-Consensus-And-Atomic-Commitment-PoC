package com.example.common

import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.gpac.domain.GPACProtocol
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
            val change = ChangeDto(call.receive())
            consensusProtocol.proposeChange(change.toChange(), null)
            call.respond(HttpStatusCode.OK)
        }

    }

}