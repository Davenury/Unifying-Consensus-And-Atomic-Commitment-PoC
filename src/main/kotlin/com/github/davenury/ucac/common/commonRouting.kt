package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.gpac.domain.TransactionResult
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.commonRoutingOld(
    gpacProtocol: GPACProtocol,
    consensusProtocol: ConsensusProtocol,
) {

    routing {

        get("/gpac/change_status/{transaction_id}"){
            val transactionId: String = call.parameters["transaction_id"]!!
            val transactionResult = gpacProtocol.getChangeStatus(transactionId)
            call.respond(transactionResult)
        }

        post("/gpac/create_change") {
            val change = call.receive<Change>()
            val statusCode = when (gpacProtocol.performProtocolAsLeader(change)) {
                TransactionResult.DONE -> HttpStatusCode.OK
                TransactionResult.FAILED -> HttpStatusCode.BadRequest
                TransactionResult.PROCESSED -> HttpStatusCode.Created
            }
            call.respond(statusCode)
        }

        post("/consensus/create_change/sync") {
            val change = call.receive<Change>()
            consensusProtocol.proposeChangeAsync(change).await()
            call.respond(HttpStatusCode.OK)
        }

        post("/consensus/create_change/async") {
            val change = call.receive<Change>()
            consensusProtocol.proposeChangeAsync(change)
            call.respond(HttpStatusCode.Accepted)
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
