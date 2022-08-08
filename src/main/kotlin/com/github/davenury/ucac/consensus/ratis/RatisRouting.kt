package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.common.*
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.ratisRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                val result = historyManagement.getLastChange()
                call.respond((result?.toDto() ?: ErrorMessage("Error")))
            }
        }
        route("/changes") {
            get {
                val result = historyManagement.getState()
                call.respond((result?.toDto() ?: ErrorMessage("Error")))
            }
        }
    }
}


