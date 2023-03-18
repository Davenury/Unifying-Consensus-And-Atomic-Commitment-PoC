package com.github.davenury.ucac.routing

import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.consensus.ConsensusProposeChange
import com.github.davenury.ucac.consensus.pigpaxos.*
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.pigPaxosProtocolRouting(protocol: PigPaxosProtocol) {
    routing {
        // głosujemy na leadera
        post("/pigpaxos/propose") {
            val message: PaxosPropose = call.receive()
            val response = protocol.handlePropose(message)
            call.respond(response)
        }

        post("/pigpaxos/accept") {
            val message: PaxosAccept = call.receive()
            val heartbeatResult = protocol.handleAccept(message)
            call.respond(heartbeatResult)
        }

        post("/pigpaxos/commit") {
            val message: PaxosCommit = call.receive()
            val heartbeatResult = protocol.handleCommit(message)
            call.respond(heartbeatResult)
        }

        post("/pigpaxos/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = protocol.handleProposeChange(message).await()
                .also {
                    ChangeNotifier.notify(message, it)
                }
            call.respond(result)
        }

        get("/pigpaxos/current-leader") {
            call.respond(CurrentLeaderDto(protocol.getLeaderId()))
        }

        get("/pigpaxos/proposed_changes") {
            call.respond(Changes(protocol.getProposedChanges()))
        }

        get("/pigpaxos/accepted_changes") {
            call.respond(Changes(protocol.getAcceptedChanges()))
        }
    }
}
