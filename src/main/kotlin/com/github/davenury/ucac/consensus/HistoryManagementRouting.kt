package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.history.InitialHistoryEntry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.historyManagementRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                val result = historyManagement.getLastChange()
                if (result == InitialHistoryEntry) {
                    call.respond(HttpStatusCode.NotFound)
                } else {
                    call.respond(Change.fromHistoryEntry(result))
                }
            }
        }
        route("/changes") {
            get {
                val result = historyManagement.getState().toEntryList()
                call.respond(Changes(result.map { Change.fromHistoryEntry(it) }))
            }
        }
    }
}
