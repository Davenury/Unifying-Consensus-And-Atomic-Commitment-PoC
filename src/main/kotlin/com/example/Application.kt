package com.example

import com.example.api.configureRouting
import com.example.domain.ErrorMessage
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

fun main(args: Array<String>) {

    val id = args[0].toInt()
    embeddedServer(Netty, port = 8080 + id, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
        }

        install(Koin) {
            modules(historyManagementModule(id))
        }

        install(StatusPages) {
            exception<MissingParameterException> { cause ->
                call.respond(status = HttpStatusCode.BadRequest, ErrorMessage("Missing parameter: ${cause.message}"))
            }
            exception<UnknownOperationException> { cause ->
                call.respond(
                    status = HttpStatusCode.BadRequest,
                    ErrorMessage("Unknown operation to perform: ${cause.desiredOperationName}")
                )
            }
        }

        configureRouting()
    }.start(wait = true)
}
