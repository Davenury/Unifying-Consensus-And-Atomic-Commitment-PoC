package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
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
            call.respond(Changes.fromHistory(consensusProtocol.getState()))
        }

        get("/consensus/change") {
            call.respond(consensusProtocol.getState()
                .getCurrentEntry()
                .takeIf { it != InitialHistoryEntry }
                ?.let { Change.fromHistoryEntry(it) }
                ?: HttpStatusCode.NotFound)
        }

    }

}
