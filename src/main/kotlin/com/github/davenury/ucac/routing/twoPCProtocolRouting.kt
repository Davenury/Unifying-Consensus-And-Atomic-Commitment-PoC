package com.github.davenury.ucac.routing

import com.github.davenury.common.Change
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.twoPCRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.twoPC(): TwoPC {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).twoPC
    }

    routing {
        post("/2pc/accept") {
            val message = call.receive<Change>()
            call.twoPC().handleAccept(message)
            call.respond(HttpStatusCode.OK)
        }
        post("/2pc/decision") {
            val message = call.receive<Change>()
            call.twoPC().handleDecision(message)
            call.respond(HttpStatusCode.OK)
        }

        get("/2pc/ask/{changeId}") {
            val id = call.parameters["changeId"]!!
            val change = call.twoPC().getChange(id)
            call.respond(HttpStatusCode.OK, change)
        }
    }
}
