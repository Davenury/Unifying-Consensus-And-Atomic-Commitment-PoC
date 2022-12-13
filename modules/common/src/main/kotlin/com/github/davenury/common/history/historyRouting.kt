package com.github.davenury.common.history

import com.github.davenury.common.*
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
                    ?.let { Transition.fromHistoryEntry(it) }
                    ?.let { it as? ChangeApplyingTransition }
                    ?.change
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
        route("/transitions") {
            get {
                call.respond(Transitions.fromHistory(history))
            }
        }
    }
}
