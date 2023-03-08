package com.github.davenury.ucac.routing

import com.github.davenury.ucac.commitment.gpac.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

fun Application.gpacProtocolRouting(
    queue: GPACQueue,
) {

    routing {

        post("/elect") {
            val message = call.receive<ElectMe>()
            val returnUrl = call.parameters["leader-return-address"]!!
            queue.handleElect(message, URLDecoder.decode(returnUrl, StandardCharsets.UTF_8))
            call.respond(HttpStatusCode.OK)
        }

        post("/ft-agree") {
            val message = call.receive<Agree>()
            val returnUrl = call.parameters["leader-return-address"]!!
            queue.handleFtAgree(message, URLDecoder.decode(returnUrl, StandardCharsets.UTF_8))
            call.respond(HttpStatusCode.OK)
        }

        post("/apply") {
            val message = call.receive<Apply>()
            queue.handleApply(message)
            call.respond(HttpStatusCode.OK)
        }

        post("/gpac/leader/elect-response") {
            queue.handleElectedYou(call.receive())
            call.respond(HttpStatusCode.OK)
        }

        post("/gpac/leader/agree-response") {
            queue.handleAgreed(call.receive())
            call.respond(HttpStatusCode.OK)
        }

        post("/gpac/leader/apply-response") {
            call.respond(HttpStatusCode.OK)
        }
    }

}
