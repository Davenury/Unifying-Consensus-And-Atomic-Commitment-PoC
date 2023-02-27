package com.github.davenury.common.history

import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.common.ErrorMessage
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
                val changes = Changes.fromHistory(history)
                log.info("Changes: $changes")
                call.respond(changes)
            }
        }
    }
}
