package com.example

import com.example.api.configureSampleRouting
import com.example.api.protocolRouting
import com.example.domain.*
import com.example.infrastructure.RatisHistoryManagement
import com.example.raft.HistoryRaftNode
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*

fun main(args: Array<String>) {

    val conf = getIdAndOffset(args)
    val raftNode = HistoryRaftNode(conf.nodeId)
    val historyManagement = RatisHistoryManagement(raftNode)
    val protocol = GPACProtocolImpl(historyManagement)
    embeddedServer(Netty, port = 8080 + conf.portOffset, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
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
            exception<NotElectingYou> { cause ->
                call.respond(status = HttpStatusCode.UnprocessableEntity, ErrorMessage("You're not valid leader. My Ballot Number is: ${cause.ballotNumber}"))
            }
            exception<Throwable> { cause ->
                call.respond(
                    status = HttpStatusCode.InternalServerError,
                    ErrorMessage("UnexpectedError, $cause")
                )
            }
        }

        configureSampleRouting(historyManagement)
        protocolRouting(protocol)
    }.start(wait = true)
}

data class NodeIdAndPortOffset(
    val nodeId: Int,
    val portOffset: Int
)

fun getIdAndOffset(args: Array<String>): NodeIdAndPortOffset {
    if (args.isNotEmpty()) {
        return NodeIdAndPortOffset(nodeId = args[0].toInt(), portOffset = args[0].toInt())
    }
    
    val id = System.getenv()["RAFT_NODE_ID"]?.toInt() ?: throw RuntimeException("Provide either arg or RAFT_NODE_ID env variable to represent id of node")

    return NodeIdAndPortOffset(nodeId = id, portOffset = 0)
}
