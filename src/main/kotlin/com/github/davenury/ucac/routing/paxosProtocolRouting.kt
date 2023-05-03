package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProposeChange
import com.github.davenury.ucac.consensus.paxos.*
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.pigPaxosProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): PaxosProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as PaxosProtocol
    }
    routing {
        post("/paxos/propose") {
            val message: PaxosPropose = call.receive()
            val response = call.consensus().handlePropose(message)
            call.respond(response)
        }

        post("/paxos/accept") {
            val message: PaxosAccept = call.receive()
            val heartbeatResult = call.consensus().handleAccept(message)
            call.respond(heartbeatResult)
        }

        post("/paxos/commit") {
            val message: PaxosCommit = call.receive()
            val heartbeatResult = call.consensus().handleCommit(message)
            call.respond(heartbeatResult)
        }

        post("/paxos/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = call.consensus().handleProposeChange(message).await()
            call.respond(result)
        }


        get("/paxos/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }

        get("/paxos/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
