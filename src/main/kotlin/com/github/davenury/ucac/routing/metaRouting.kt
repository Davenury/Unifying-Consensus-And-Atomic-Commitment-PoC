package com.github.davenury.ucac.routing

import com.github.davenury.ucac.meterRegistry
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.metaRouting() {

    routing {
        get("/_meta/metrics") { call.respond(meterRegistry.scrape()) }

        get("/_meta/health") { call.respond(HttpStatusCode.OK) }
    }

}
