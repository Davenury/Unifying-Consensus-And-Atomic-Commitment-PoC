package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.meterRegistry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlin.random.Random

fun Application.commonRouting(
    gpacProtocol: GPACProtocol,
    consensusProtocol: ConsensusProtocol<Change, History>,
) {

    val counter = meterRegistry.counter("test_counter")

    routing {

        post("/create_change") {
            val change = call.receive<ChangeDto>()
            gpacProtocol.performProtocolAsLeader(change)
            call.respond(HttpStatusCode.OK)
        }

        post("/consensus/create_change") {
            val properties = call.receive<Map<String, Any>>()
            val change = ChangeDto(properties["change"] as Map<String, String>, properties["peers"] as List<String>)
            consensusProtocol.proposeChange(change.toChange(), properties["acceptNum"] as Int?)
            call.respond(HttpStatusCode.OK)
        }
        
        get("/consensus/changes") {
            call.respond(consensusProtocol.getState()?.toDto() ?: listOf<ChangeWithAcceptNumDto>())
        }
        get("/consensus/change") {
            call.respond(consensusProtocol.getState()?.lastOrNull()?.toDto() ?: HttpStatusCode.BadRequest)
        }

        post("/bump/counter") {
            counter.increment()
            call.respond(HttpStatusCode.OK)
        }

        post("/random_fail") {

            val result = Random.nextInt(10)
            if (result % 4 == 0) {
                call.respond(HttpStatusCode.BadRequest)
            } else if (result % 3 == 0) {
                call.respond(HttpStatusCode.ServiceUnavailable)
            } else {
                call.respond(HttpStatusCode.OK)
            }

        }
    }

}