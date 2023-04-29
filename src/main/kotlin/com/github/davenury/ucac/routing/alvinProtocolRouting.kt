package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.CurrentLeaderDto
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.alvin.*
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocol
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.alvinProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): AlvinProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as AlvinProtocol
    }
    routing {
        post("/alvin/proposal") {
            val message: AlvinPropose = call.receive()
            val response = call.consensus().handleProposalPhase(message)
            call.respond(response)
        }

        post("/alvin/accept") {
            val message: AlvinAccept = call.receive()
            val response = call.consensus().handleAcceptPhase(message)
            call.respond(response)
        }

        post("/alvin/stable") {
            val message: AlvinStable = call.receive()
            val result = call.consensus().handleStable(message)
            call.respond(result)
        }

        post("/alvin/prepare") {
            val message: AlvinAccept = call.receive()
            val result = call.consensus().handlePrepare(message)
            call.respond(result)
        }

        post("/alvin/commit") {
            val message: AlvinCommit = call.receive()
            val result = call.consensus().handleCommit(message)
            call.respond(HttpStatusCode.OK,result)
        }

        post("/alvin/fast-recovery") {
            val message: AlvinFastRecovery = call.receive()
            val result = call.consensus().handleFastRecovery(message)
            call.respond(result)
        }

        get("/consensus/current-leader") {
            call.respond(CurrentLeaderDto(call.consensus().getLeaderId()))
        }

        get("/alvin/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }
        get("/alvin/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
