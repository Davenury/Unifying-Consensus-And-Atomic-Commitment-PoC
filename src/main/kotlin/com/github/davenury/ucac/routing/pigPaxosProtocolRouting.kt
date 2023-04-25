package com.github.davenury.ucac.routing

import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.common.CurrentLeaderDto
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProposeChange
import com.github.davenury.ucac.consensus.pigpaxos.*
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocol
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.pigPaxosProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): PigPaxosProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as PigPaxosProtocol
    }
    routing {
        // głosujemy na leadera
        post("/pigpaxos/propose") {
            val message: PaxosPropose = call.receive()
            val response = call.consensus().handlePropose(message)
            call.respond(response)
        }

        post("/pigpaxos/accept") {
            val message: PaxosAccept = call.receive()
            val heartbeatResult = call.consensus().handleAccept(message)
            call.respond(heartbeatResult)
        }

        post("/pigpaxos/commit") {
            val message: PaxosCommit = call.receive()
            val heartbeatResult = call.consensus().handleCommit(message)
            call.respond(heartbeatResult)
        }

        post("/pigpaxos/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = call.consensus().handleProposeChange(message).await()
            call.respond(result)
        }

        get("/pigpaxos/current-leader") {
            call.respond(CurrentLeaderDto(call.consensus().getLeaderId()))
        }

        get("/pigpaxos/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }

        get("/pigpaxos/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
