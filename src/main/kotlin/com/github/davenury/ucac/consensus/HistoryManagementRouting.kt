package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.common.*
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.historyManagementRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                val result = historyManagement.getLastChange()
                call.respond((result?.toDto() ?: ErrorMessage("Error any change doesn't exist")))
            }
        }
        route("/changes") {
            get {
                val result = historyManagement.getState()
                call.respond((result.toDto()))
            }
        }
    }
}


