package com.github.davenury.ucac.routing

import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.gpac.Agree
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.commitment.gpac.ElectMe
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.gpacProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.gpac(): GPACFactory {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).gpacFactory
    }

    routing {
        post("/elect") {
            val message = call.receive<ElectMe>()
            call.respond(call.gpac().handleElect(message))
        }

        post("/ft-agree") {
            val message = call.receive<Agree>()
            call.respond(call.gpac().handleAgree(message))
        }

        post("/apply") {
            val message = call.receive<Apply>()
            call.gpac().handleApply(message)
            call.respond(HttpStatusCode.OK)
        }
    }
}
