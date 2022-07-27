package github.davenury.ucac

import github.davenury.ucac.common.*
import github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import github.davenury.ucac.consensus.ratis.HistoryRaftNode
import github.davenury.ucac.consensus.ratis.RaftConfiguration
import github.davenury.ucac.consensus.ratis.RatisHistoryManagement
import github.davenury.ucac.consensus.ratis.ratisRouting
import github.davenury.ucac.gpac.api.gpacProtocolRouting
import github.davenury.ucac.gpac.domain.GPACProtocolImpl
import github.davenury.ucac.gpac.domain.ProtocolClientImpl
import github.davenury.ucac.gpac.domain.TransactionBlockerImpl
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.response.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.netty.channel.socket.nio.NioServerSocketChannel
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

fun main(args: Array<String>) {
    createApplication(args).startBlocking()
}

fun startApplication(
    args: Array<String>,
    additionalActions: Map<TestAddon, AdditionalAction> = emptyMap(),
    eventListeners: List<EventListener> = emptyList(),
    configOverrides: Map<String, Any> = emptyMap()
) {
    createApplication(args, additionalActions, eventListeners, configOverrides).startBlocking()
}

fun createApplication(
    args: Array<String>,
    additionalActions: Map<TestAddon, AdditionalAction> = emptyMap(),
    eventListeners: List<EventListener> = emptyList(),
    configOverrides: Map<String, Any> = emptyMap()
): Application {
    return Application(args, additionalActions, eventListeners, configOverrides)
}

data class NodeIdAndPortOffset(val nodeId: Int, val portOffset: Int, val peersetId: Int)

fun getIdAndOffset(args: Array<String>, config: Config): NodeIdAndPortOffset {

    if (args.isNotEmpty()) {
        val peersetId = args[1].toInt()
        val portOffsetFromPreviousPeersets: Int =
            config.peers.peersAddresses.foldIndexed(0) { index, acc, strings ->
                if (index <= peersetId - 2) acc + strings.size else acc + 0
            }
        return NodeIdAndPortOffset(
            nodeId = args[0].toInt(),
            portOffset = args[0].toInt() + portOffsetFromPreviousPeersets,
            peersetId
        )
    }

    val peersetId =
        System.getenv()["PEERSET_ID"]?.toInt()
            ?: throw RuntimeException(
                "Provide PEERSET_ID env variable to represent id of node"
            )

    val id =
        System.getenv()["RAFT_NODE_ID"]?.toInt()
            ?: throw RuntimeException(
                "Provide either arg or RAFT_NODE_ID env variable to represent id of node"
            )

    return NodeIdAndPortOffset(nodeId = id, portOffset = 0, peersetId)
}

fun getOtherPeers(
    peersAddresses: List<List<String>>,
    peerOffset: Int,
    peersetId: Int,
    basePort: Int = 8080
): List<List<String>> =
    try {
        peersAddresses.foldIndexed(mutableListOf()) { index, acc, strings ->
            if (index == peersetId - 1) {
                acc +=
                    strings.filterNot {
                        it.contains("peer$peerOffset") || it.contains("${basePort + peerOffset}")
                    }
                acc
            } else {
                acc += strings
                acc
            }
        }
    } catch (e: java.lang.IndexOutOfBoundsException) {
        println(
            "Peers addresses doesn't have enough elements in list - peers addresses length: ${peersAddresses.size}, index: ${peersetId - 1}"
        )
        throw IllegalStateException()
    }

class Application constructor(
    args: Array<String>,
    private val additionalActions: Map<TestAddon, AdditionalAction>,
    private val eventListeners: List<EventListener>,
    configOverrides: Map<String, Any>
) {
    private val config: Config = loadConfig(configOverrides)
    private val conf: NodeIdAndPortOffset = getIdAndOffset(args, config)
    private val peerConstants: RaftConfiguration = RaftConfiguration(conf.peersetId, configOverrides)
    private val engine: NettyApplicationEngine
    private var raftNode: HistoryRaftNode? = null
    private lateinit var ctx: ExecutorCoroutineDispatcher

    init {
        engine = embeddedServer(Netty, port = 8080 + conf.portOffset, host = "0.0.0.0") {
            raftNode = HistoryRaftNode(conf.nodeId, conf.peersetId, peerConstants)

            val otherPeers = getOtherPeers(config.peers.peersAddresses, conf.portOffset, conf.peersetId)

            val nodeIdOffset: Int = otherPeers.take(conf.peersetId - 1).map { it.size }.sum()

            ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

            val consensusProtocol = RaftConsensusProtocolImpl(
                conf.nodeId,
                conf.peersetId,
                ProtocolTimerImpl(Duration.ZERO, Duration.ofSeconds(1), ctx), // TODO move to config
                otherPeers[conf.peersetId - 1]
            )

            val historyManagement = RatisHistoryManagement(raftNode!!)
//            val historyManagement = InMemoryHistoryManagement(consensusProtocol)
            val eventPublisher = EventPublisher(eventListeners)
            val timer = ProtocolTimerImpl(config.protocol.leaderFailTimeout, config.protocol.backoffBound, ctx)
            val protocolClient = ProtocolClientImpl()
            val transactionBlocker = TransactionBlockerImpl()
            val gpacProtocol =
                GPACProtocolImpl(
                    historyManagement,
                    config.peers.maxLeaderElectionTries,
                    timer,
                    protocolClient,
                    transactionBlocker,
                    otherPeers,
                    additionalActions,
                    eventPublisher,
                    8080 + conf.portOffset,
                    conf.peersetId
                )

            install(ContentNegotiation) {
                register(ContentType.Application.Json, JacksonConverter(objectMapper))
            }


            install(StatusPages) {
                exception<MissingParameterException> { cause ->
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage("Missing parameter: ${cause.message}")
                    )
                }
                exception<UnknownOperationException> { cause ->
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage(
                            "Unknown operation to perform: ${cause.desiredOperationName}"
                        )
                    )
                }
                exception<NotElectingYou> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage(
                            "You're not valid leader-to-be. My Ballot Number is: ${cause.ballotNumber}, whereas provided was ${cause.messageBallotNumber}"
                        )
                    )
                }
                exception<NotValidLeader> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage(
                            "You're not valid leader. My Ballot Number is: ${cause.ballotNumber}, whereas provided was ${cause.messageBallotNumber}"
                        )
                    )
                }
                exception<MaxTriesExceededException> {
                    call.respond(
                        HttpStatusCode.ServiceUnavailable,
                        ErrorMessage(
                            "Transaction failed due to too many retries of becoming a leader."
                        )
                    )
                }
                exception<TooFewResponsesException> {
                    call.respond(
                        HttpStatusCode.ServiceUnavailable,
                        ErrorMessage(
                            "Transaction failed due to too few responses of ft phase."
                        )
                    )
                }
                exception<HistoryCannotBeBuildException> {
                    call.respond(
                        HttpStatusCode.BadRequest,
                        ErrorMessage(
                            "Change you're trying to perform is not applicable with current state"
                        )
                    )
                }
                exception<AlreadyLockedException> {
                    call.respond(
                        HttpStatusCode.Conflict,
                        ErrorMessage(
                            "We cannot perform your transaction, as another transaction is currently running"
                        )
                    )
                }
                exception<Throwable> { cause ->
                    call.respond(
                        status = HttpStatusCode.InternalServerError,
                        ErrorMessage("UnexpectedError, $cause")
                    )
                }
            }

            commonRouting(gpacProtocol, consensusProtocol)
            ratisRouting(historyManagement)
            gpacProtocolRouting(gpacProtocol)
            //consensusProtocolRouting(consensusProtocol)

//        runBlocking {
//            consensusProtocol.begin()
//        }
        }
    }

    fun startBlocking() {
        engine.start(wait = true)
    }

    fun startNonblocking() {
        engine.start(wait = false)
    }

    fun stop(gracePeriodMillis: Long = 200, timeoutMillis: Long = 1000) {
        engine.stop(gracePeriodMillis, timeoutMillis)
        raftNode?.close()
    }

    fun getBoundPort(): Int {
        val channelsProperty =
            NettyApplicationEngine::class.declaredMemberProperties.single { it.name == "channels" }
        val oldAccessible = channelsProperty.isAccessible
        try {
            channelsProperty.isAccessible = true
            val channels = channelsProperty.get(engine) as List<*>
            val channel = channels.single() as NioServerSocketChannel
            return channel.localAddress()!!.port
        } finally {
            channelsProperty.isAccessible = oldAccessible
        }
    }
}

