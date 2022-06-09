package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.ErrorMessage
import com.example.domain.HistoryManagement
import com.example.objectMapper
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.delay

fun Application.configureSampleRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            post {
                val change = ChangeDto(call.receive())
                val result = historyManagement.change(change.toChange(), 1)
                call.respond(result.toString())
            }
            get {
                val result = historyManagement.getLastChange()
                call.respond((result ?: ErrorMessage("Error")).let { objectMapper.writeValueAsString(it) })
            }
        }
    }
}

