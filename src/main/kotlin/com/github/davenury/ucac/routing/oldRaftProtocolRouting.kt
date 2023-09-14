package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.oldRaft.ConsensusElectMe
import com.github.davenury.ucac.consensus.oldRaft.ConsensusHeartbeat
import com.github.davenury.ucac.consensus.oldRaft.ConsensusProposeChange
import com.github.davenury.ucac.consensus.oldRaft.OldRaftConsensusProtocol
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.oldRaftProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): OldRaftConsensusProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as OldRaftConsensusProtocol
    }
    routing {
        post("/consensus/request_vote") {
            val message: ConsensusElectMe = call.receive()
            val response = call.consensus().handleRequestVote(message.peerId, message.term, message.lastEntryId)
            call.respond(response)
        }

        post("/consensus/heartbeat") {
            val message: ConsensusHeartbeat = call.receive()
            val heartbeatResult = call.consensus().handleHeartbeat(message)
            call.respond(heartbeatResult)
        }

        post("/consensus/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = call.consensus().handleProposeChange(message).await()
            call.respond(result)
        }

        get("/consensus/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }

        get("/consensus/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}