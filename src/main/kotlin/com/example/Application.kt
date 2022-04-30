package com.example

import com.example.api.configureRouting
import io.ktor.application.*
import io.ktor.features.*
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

        configureRouting()
    }.start(wait = true)
}
