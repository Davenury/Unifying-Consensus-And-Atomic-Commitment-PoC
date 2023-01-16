package com.github.davenury.ucac.routing

import com.github.davenury.ucac.commitment.gpac.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.gpacProtocolRouting(factory: GPACFactory) {

    routing {

        post("/elect") {
            val message = call.receive<ElectMe>()
            call.respond(factory.handleElect(message))
        }

        post("/ft-agree") {
            val message = call.receive<Agree>()
            call.respond(factory.handleAgree(message))
        }

        post("/apply") {
            val message = call.receive<Apply>()
            factory.handleApply(message)
            call.respond(HttpStatusCode.OK)
        }
    }

}
