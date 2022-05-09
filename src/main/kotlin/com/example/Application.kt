package com.example

import com.example.api.configureRouting
import com.example.domain.*
import com.example.infrastructure.RatisHistoryManagement
import com.example.raft.Constants
import com.example.raft.HistoryRaftNode
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.ratis.protocol.RaftPeer
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse


fun main(args: Array<String>) {

    val conf = getIdAndOffset(args)
    val configRaftPeer = Constants.PEERS.getOrNull(conf.nodeId)

    val peerAndPeers: Pair<RaftPeer, List<RaftPeer>> = if (configRaftPeer == null) {
        val leaderResponse = askLeader(conf.nodeId)
        informLeader(leaderResponse.first)
        leaderResponse
    } else Pair(configRaftPeer, Constants.PEERS)


    println("$peerAndPeers\n\n\n\n\n")
    val raftNode = HistoryRaftNode(peerAndPeers.first, peerAndPeers.second)
    val historyManagement = RatisHistoryManagement(raftNode)
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
            exception<Throwable> { cause ->
                call.respond(
                    status = HttpStatusCode.InternalServerError,
                    ErrorMessage("UnexpectedError, $cause")
                )
            }
        }

        configureRouting(historyManagement, raftNode)
    }.start(wait = true)
}

fun askLeader(nodeId: Int): Pair<RaftPeer, List<RaftPeer>> {
    val leader = Constants.PEERS[0]
    val request = HttpRequest.newBuilder().uri(URI.create("http://${leader.address}/config")).build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    val peers: List<RaftPeer> = objectMapper
        .readValue(response.body(), ArrayList<HashMap<String, String>>().javaClass)
        .map { RaftPeerDto.fromJson(it) }

    val newPort = peers.last().address.split(":").last().toInt() + 1

    val newPeer = RaftPeer.newBuilder().setId(nodeId.toString()).setAddress("localhost:${newPort}").build()

    return Pair(newPeer, peers)
}

fun informLeader(raftPeer: RaftPeer) {
    val leader = Constants.PEERS[0]
    val request = HttpRequest
        .newBuilder()
        .uri(URI.create("http://${leader.address}/add_peer"))
        .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(raftPeer)))
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    LoggerFactory.getLogger(ChangeDto::class.java).info(response.body())

}

data class NodeIdAndPortOffset(
    val nodeId: Int,
    val portOffset: Int
)

fun getIdAndOffset(args: Array<String>): NodeIdAndPortOffset {
    if (args.isNotEmpty()) {
        return NodeIdAndPortOffset(nodeId = args[0].toInt(), portOffset = args[0].toInt())
    }

    val id = System.getenv()["RAFT_NODE_ID"]?.toInt()
        ?: throw RuntimeException("Provide either arg or RAFT_NODE_ID env variable to represent id of node")

    return NodeIdAndPortOffset(nodeId = id, portOffset = 0)
}
