package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.history.IntermediateHistoryEntry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import java.lang.IllegalArgumentException

fun Application.commonRouting(
    gpacProtocol: GPACProtocol,
    consensusProtocol: ConsensusProtocol,
) {

    routing {

        post("/create_change") {
            val change = call.receive<Change>()
            gpacProtocol.performProtocolAsLeader(change)
            call.respond(HttpStatusCode.OK)
        }

        post("/consensus/create_change") {
            val change = call.receive<Change>()
            consensusProtocol.proposeChange(change.toHistoryEntry())
            call.respond(HttpStatusCode.OK)
        }

        get("/consensus/changes") {
            val history = consensusProtocol.getState()
            val changes = generateSequence(history.getCurrentEntry()) {
                val parentId = it.getParentId()
                if (parentId != null) {
                    history.getEntryFromHistory(parentId)!!
                } else {
                    null
                }
            }
            call.respond(changes.map { Change.fromHistoryEntry(it) })
        }
        get("/consensus/change") {
            call.respond(consensusProtocol.getState().getCurrentEntry().serialize())
        }
    }

}
