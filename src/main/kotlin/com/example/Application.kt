package com.example

import com.example.api.configureSampleRouting
import com.example.api.protocolRouting
import com.example.domain.*
import com.example.infrastructure.ProtocolTimerImpl
import com.example.infrastructure.RatisHistoryManagement
import com.example.raft.Constants
import com.example.raft.HistoryRaftNode
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.response.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*


fun main(args: Array<String>) {
    startApplication(args)
}

fun startApplication(
    args: Array<String>,
    additionalActions: Map<TestAddon, AdditionalAction> = emptyMap(),
    eventListeners: List<EventListener> = emptyList(),
    configOverrides: Map<String, Any> = emptyMap()
) {
    val config = loadConfig(configOverrides)
    val conf = getIdAndOffset(args, config)
    Constants.loadConfig(conf.peersetId, configOverrides)
    embeddedServer(Netty, port = 8080 + conf.portOffset, host = "0.0.0.0") {

        val raftNode = HistoryRaftNode(conf.nodeId, conf.peersetId)
        val historyManagement = RatisHistoryManagement(raftNode)
        val eventPublisher = EventPublisher(eventListeners)
        val timer = ProtocolTimerImpl(config.protocol.leaderFailTimeoutInSecs)
        val protocolClient = ProtocolClientImpl()
        val transactionBlocker = TransactionBlockerImpl()
        val otherPeers = getOtherPeers(config.peers.peersAddresses, conf.nodeId, conf.peersetId)
        val protocol =
            GPACProtocolImpl(
                historyManagement,
                config.peers.maxLeaderElectionTries,
                timer,
                protocolClient,
                transactionBlocker,
                otherPeers,
                additionalActions,
                eventPublisher,
                8080 + conf.portOffset
            )

        install(ContentNegotiation) {
            register(ContentType.Application.Json, JacksonConverter(objectMapper))
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
                call.respond(
                    status = HttpStatusCode.UnprocessableEntity,
                    ErrorMessage("You're not valid leader. My Ballot Number is: ${cause.ballotNumber}")
                )
            }
            exception<MaxTriesExceededException> {
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    ErrorMessage("Transaction failed due to too many retries of becoming a leader.")
                )
            }
            exception<TooFewResponsesException> {
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    ErrorMessage("Transaction failed due to too few responses of ft phase.")
                )
            }
            exception<HistoryCannotBeBuildException> {
                call.respond(
                    HttpStatusCode.BadRequest,
                    ErrorMessage("Change you're trying to perform is not applicable with current state")
                )
            }
            exception<AlreadyLockedException> {
                call.respond(
                    HttpStatusCode.Conflict,
                    ErrorMessage("We cannot perform your transaction, as another transaction is currently running")
                )
            }
//            exception<Throwable> { cause ->
//                call.respond(
//                    status = HttpStatusCode.InternalServerError,
//                    ErrorMessage("UnexpectedError, $cause")
//                )
//            }
        }

        configureSampleRouting(historyManagement)
        protocolRouting(protocol)
    }.start(wait = true)
}

data class NodeIdAndPortOffset(
    val nodeId: Int,
    val portOffset: Int,
    val peersetId: Int
)

fun getIdAndOffset(args: Array<String>, config: Config): NodeIdAndPortOffset {

    if (args.isNotEmpty()) {
        val peersetId = args[1].toInt()
        val portOffsetFromPreviousPeersets: Int =
            config.peers.peersAddresses.foldIndexed(0) { index, acc, strings -> if (index <= peersetId - 2) acc + strings.size  else acc + 0 }
        return NodeIdAndPortOffset(nodeId = args[0].toInt(), portOffset = args[0].toInt() + portOffsetFromPreviousPeersets, peersetId)
    }

    val peersetId = System.getenv()["PEERSET_ID"]?.toInt()
        ?: throw RuntimeException("Provide PEERSET_ID env variable to represent id of node")

    val id = System.getenv()["RAFT_NODE_ID"]?.toInt()
        ?: throw RuntimeException("Provide either arg or RAFT_NODE_ID env variable to represent id of node")

    return NodeIdAndPortOffset(nodeId = id, portOffset = 0, peersetId)
}

fun getOtherPeers(peersAddresses: List<List<String>>, nodeId: Int, peersetId: Int): List<List<String>> =
    try {
        peersAddresses.foldIndexed(mutableListOf()) { index, acc, strings ->
            if (index == peersetId - 1) {
                acc += strings.filterNot { it.contains("peer$nodeId") || it.contains("${8080 + nodeId}") }
                acc
            } else {
                acc += strings
                acc
            }
        }
    } catch (e: java.lang.IndexOutOfBoundsException) {
        println("Peers addresses doesn't have enough elements in list - peers addresses length: ${peersAddresses.size}, index: ${peersetId - 1}")
        throw IllegalStateException()
    }
