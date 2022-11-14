package com.github.davenury.ucac.routing

import com.github.davenury.ucac.commitment.TwoPC.TwoPC
import com.github.davenury.ucac.commitment.gpac.ElectMe
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.common.ErrorMessage
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlin.text.get

fun Application.twoPCRouting(twoPC: TwoPC) {
    // Starting point for a Ktor app:
    routing {
        post("/2pc/accept") {
            val message = call.receive<Change>()
            twoPC.handleAccept(message)
            call.respond(HttpStatusCode.OK)
        }
        post("/2pc/commit") {
            val message = call.receive<Change>()
            twoPC.handleCommit(message)
            call.respond(HttpStatusCode.OK)
        }
        post("/2pc/decision") {
            val message = call.receive<Change>()
            twoPC.handleDecision(message)
            call.respond(HttpStatusCode.OK)
        }
    }
}