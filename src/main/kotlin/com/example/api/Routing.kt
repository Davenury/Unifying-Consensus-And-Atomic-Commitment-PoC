package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.HistoryManagement
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.apache.ratis.thirdparty.com.google.gson.Gson
import org.koin.ktor.ext.inject

fun Application.configureRouting(historyManagement: HistoryManagement) {

    val historyManagementFacade: HistoryManagementFacade by inject()


    // Starting point for a Ktor app:
    routing {
        post("/change") {
            val change = ChangeDto(call.receive())
            val result = historyManagement.change(change.toChange())
            call.respond(result.toString())
        }
        get("/change") {
            val result = historyManagement.getLastChange()
            call.respond(result?.let { Gson().toJson(it) } ?: "Error")
        }

    }
}

