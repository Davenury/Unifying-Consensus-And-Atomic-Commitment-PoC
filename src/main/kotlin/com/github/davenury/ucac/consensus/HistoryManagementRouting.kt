package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.common.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.historyManagementRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                historyManagement.getLastChange()
                    ?.let { call.respond(it) }
                    ?: call.respond(
                        HttpStatusCode.NotFound,
                        ErrorMessage("Error any change doesn't exist")
                    )
            }
        }
        route("/changes") {
            get {
                val result = historyManagement.getState()
                call.respond(Changes.fromHistory(result))
            }
        }
    }
}
