package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.ucac.consensus.alvin.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.alvinProtocolRouting(protocol: AlvinProtocol) {
    routing {
        post("/alvin/proposal") {
            val message: AlvinPropose = call.receive()
            val response = protocol.handleProposalPhase(message)
            call.respond(response)
        }

        post("/alvin/accept") {
            val message: AlvinAccept = call.receive()
            val response = protocol.handleAcceptPhase(message)
            call.respond(response)
        }

        // kiedy nie jesteś leaderem to prosisz leadera o zmianę
        post("/alvin/stable") {
            val message: AlvinStable = call.receive()
            val result = protocol.handleStable(message)
            call.respond(result)
        }

        post("/alvin/prepare") {
            val message: AlvinAccept = call.receive()
            val result = protocol.handlePrepare(message)
            call.respond(result)
        }

        post("/alvin/commit") {
            val message: AlvinCommit = call.receive()
            val result = protocol.handleCommit(message)
            call.respond(HttpStatusCode.OK,result)
        }

        post("/alvin/fast-recovery") {
            val message: AlvinFastRecovery = call.receive()
            val result = protocol.handleFastRecovery(message)
            call.respond(result)
        }


//      Endpoints for tests
        get("/alvin/proposed_changes") {
            call.respond(Changes(protocol.getProposedChanges()))
        }
        get("/alvin/accepted_changes") {
            call.respond(Changes(protocol.getAcceptedChanges()))
        }
    }
}
