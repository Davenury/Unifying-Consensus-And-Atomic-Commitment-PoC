package com.example

import com.example.api.configureRouting
import com.example.domain.MissingParameterException
import com.example.domain.UnknownOperationException
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.koin.ktor.ext.Koin

fun main() {
    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
        }

        install(Koin) {
            modules(historyManagementModule)
        }

        install(StatusPages) {
            exception<MissingParameterException> { cause ->
                call.respondText(status = HttpStatusCode.BadRequest, text = "Missing parameter: ${cause.message}")
            }
            exception<UnknownOperationException> { cause ->
                call.respondText(status = HttpStatusCode.BadRequest, text = "Unknown operation to perform: ${cause.desiredOperationName}")
            }
        }

        configureRouting()
    }.start(wait = true)
}
