package com.example.gpac.api

import com.example.common.ChangeDto
import com.example.gpac.domain.Agree
import com.example.gpac.domain.Apply
import com.example.gpac.domain.ElectMe
import com.example.gpac.domain.GPACProtocol
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.gpacProtocolRouting(protocol: GPACProtocol) {

    routing {

        post("/elect") {
            val message = call.receive<ElectMe>()
            call.respond(protocol.handleElect(message))
        }

        post("/ft-agree") {
            val message = call.receive<Agree>()
            call.respond(protocol.handleAgree(message))
        }

        post("/apply") {
            val message = call.receive<Apply>()
            protocol.handleApply(message)
            call.respond(HttpStatusCode.OK)
        }
    }

}
