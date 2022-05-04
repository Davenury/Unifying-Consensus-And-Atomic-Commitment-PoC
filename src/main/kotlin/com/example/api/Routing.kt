package com.example.api

import com.example.domain.ChangeDto
import com.example.raft.RaftNode
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.koin.ktor.ext.inject

fun Application.configureRouting(id: Int) {

    val historyManagementFacade: HistoryManagementFacade by inject()
    val raftNode = RaftNode.initialize(id)


    // Starting point for a Ktor app:
    routing {
        post("/change") {
            val change = ChangeDto(call.receive())
            historyManagementFacade.change(change)
            call.respond(HttpStatusCode.OK)
        }
        post("/add"){
            raftNode.incrementValue()
            val newValue = raftNode.getValue()
            call.respond(newValue)
        }
        get("/get"){
            call.respond(raftNode.getValue())
        }
    }
}
