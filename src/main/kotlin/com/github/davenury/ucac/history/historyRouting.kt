package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.historyRouting(history: History) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                history.getCurrentEntry()
                    .takeIf { it != InitialHistoryEntry }
                    ?.let { Change.fromHistoryEntry(it) }
                    ?.let { call.respond(it) }
                    ?: call.respond(
                        HttpStatusCode.NotFound,
                        ErrorMessage("No change exists")
                    )
            }
        }
        route("/changes") {
            get {
                call.respond(Changes.fromHistory(history))
            }
        }
    }
}
