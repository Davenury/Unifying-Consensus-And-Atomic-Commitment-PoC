package com.example.api

import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import org.koin.ktor.ext.inject

fun Application.configureRouting() {

    val historyManagementFacade: HistoryManagementFacade by inject()

    // Starting point for a Ktor app:
    routing {
        get("/") {
            call.respondText("Hello World!")
        }
    }
    routing {
    }
}
