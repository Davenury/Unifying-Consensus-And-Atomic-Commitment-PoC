package com.example.api

import com.example.domain.ChangeDto
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.koin.ktor.ext.inject

fun Application.configureRouting() {

    val historyManagementFacade: HistoryManagementFacade by inject()

    // Starting point for a Ktor app:
    routing {
        post("/change") {
            val change = ChangeDto(call.receive())
            historyManagementFacade.change(change)
            call.respond(HttpStatusCode.OK)
        }
    }
}
