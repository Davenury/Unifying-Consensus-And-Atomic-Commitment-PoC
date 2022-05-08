package com.example

import com.example.api.configureRouting
import com.example.domain.MissingParameterException
import com.example.domain.UnknownOperationException
import com.example.infrastructure.RatisHistoryManagement
import com.example.raft.HistoryRaftNode
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.koin.ktor.ext.Koin
import org.koin.ktor.ext.modules

fun main(args: Array<String>) {

    val id = args[0].toInt()
    val raftNode = HistoryRaftNode(id)
    val historyManagement = RatisHistoryManagement(raftNode)
    embeddedServer(Netty, port = 8080 + id, host = "0.0.0.0") {
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
                call.respondText(
                    status = HttpStatusCode.BadRequest,
                    text = "Unknown operation to perform: ${cause.desiredOperationName}"
                )
            }
        }

        configureRouting(historyManagement)
    }.start(wait = true)
}
