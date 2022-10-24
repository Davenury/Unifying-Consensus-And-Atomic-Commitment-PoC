package com.github.davenury.ucac

import com.github.davenury.ucac.api.ApiV2Service
import com.github.davenury.ucac.api.apiV2Routing
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.consensus.ratis.HistoryRatisNode
import com.github.davenury.ucac.gpac.GPACProtocol
import com.github.davenury.ucac.gpac.GPACProtocolClientImpl
import com.github.davenury.ucac.gpac.GPACProtocolImpl
import com.github.davenury.ucac.gpac.TransactionBlockerImpl
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.routing.*
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.metrics.micrometer.*
import io.ktor.response.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.netty.channel.socket.nio.NioServerSocketChannel
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

fun main(args: Array<String>) {
    val configPrefix = "config_"
    val configOverrides = HashMap<String, String>()
    configOverrides.putAll(System.getenv()
        .filterKeys { it.startsWith(configPrefix) }
        .mapKeys { it.key.replaceFirst(configPrefix, "") }
        .mapKeys { it.key.replace("_", ".") })

    Application.logger.info("Using overrides: $configOverrides")

    val application = createApplication(configOverrides = configOverrides)
    application.startNonblocking()

    while (!Thread.interrupted()) {
        try {
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            break
        }
    }

    application.stop(5000, 5000)
}

public fun createApplication(
    signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap(),
): Application {
    val config = loadConfig(configOverrides)
    return Application(signalListeners, config)
}

public class Application constructor(
    private val signalListeners: Map<Signal, SignalListener> = emptyMap(),
    private val config: Config,
) {
    companion object {
        val logger = LoggerFactory.getLogger(Application::class.java)
    }

    private val engine: NettyApplicationEngine
    private var raftNode: HistoryRatisNode? = null
    private var consensusProtocol: RaftConsensusProtocol? = null
    private val ctx: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private lateinit var gpacProtocol: GPACProtocol

    init {
        logger.info("Starting application with config: $config")
        engine = embeddedServer(Netty, port = config.port, host = "0.0.0.0") {
            val history = History()
            val signalPublisher = SignalPublisher(signalListeners)

            val raftProtocolClientImpl = RaftProtocolClientImpl(config.peerId)

            consensusProtocol = RaftConsensusProtocolImpl(
                history,
                config.peerId,
                config.peersetId,
                config.host + ":" + config.port,
                ctx,
                config.peerAddresses()[config.peersetId],
                signalPublisher,
                raftProtocolClientImpl,
                heartbeatTimeout = config.raft.heartbeatTimeout,
                heartbeatDelay = config.raft.leaderTimeout,
            )

            val timer = ProtocolTimerImpl(config.gpac.leaderFailTimeout, config.gpac.backoffBound, ctx)
            val protocolClient = GPACProtocolClientImpl()
            val transactionBlocker = TransactionBlockerImpl()
            val myAddress = "${config.host}:${config.port}"
            gpacProtocol =
                GPACProtocolImpl(
                    history,
                    config.gpac.maxLeaderElectionTries,
                    timer,
                    protocolClient,
                    transactionBlocker,
                    signalPublisher,
                    allPeers = config.peerAddresses().withIndex().associate { it.index to it.value },
                    myPeersetId = config.peersetId,
                    myNodeId = config.peerId,
                    myAddress = myAddress
                )

            install(ContentNegotiation) {
                register(ContentType.Application.Json, JacksonConverter(objectMapper))
            }

            install(MicrometerMetrics) {
                registry = meterRegistry
                meterBinders = listOf(
                    JvmMemoryMetrics(),
                    JvmGcMetrics(),
                    ProcessorMetrics()
                )
            }

            install(StatusPages) {
                exception<MissingParameterException> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage("Missing parameter: ${cause.message}")
                    )
                }
                exception<UnknownOperationException> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
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

                exception<PeerNotInPeersetException> { cause ->
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage(cause.message!!)
                    )
                }

                exception<Throwable> { cause ->
                    log.error("Throwable has been thrown in Application: ", cause)
                    call.respond(
                        status = HttpStatusCode.InternalServerError,
                        ErrorMessage("UnexpectedError, $cause")
                    )
                }
            }

            metaRouting()

            historyRouting(history)
            apiV2Routing(
                ApiV2Service(
                    gpacProtocol,
                    consensusProtocol as RaftConsensusProtocolImpl,
                    history,
                    config,
                )
            )
            commonRoutingOld(gpacProtocol, consensusProtocol as RaftConsensusProtocolImpl)
            gpacProtocolRouting(gpacProtocol)
            consensusProtocolRouting(consensusProtocol!!)

            runBlocking {
                startConsensusProtocol()
            }
        }
    }

    fun setPeers(peers: Map<Int, List<String>>, myAddress: String) {
        gpacProtocol.setPeers(peers)
        gpacProtocol.setMyAddress(myAddress)
        consensusProtocol?.setOtherPeers(peers[config.peersetId]!!)
    }

    suspend fun startConsensusProtocol() {
        consensusProtocol?.begin()
    }

    fun startNonblocking() {
        engine.start(wait = false)
        val address = "${config.host}:${getBoundPort()}"
        consensusProtocol?.setPeerAddress(address)
    }

    fun stop(gracePeriodMillis: Long = 200, timeoutMillis: Long = 1000) {
        ctx.close()
        engine.stop(gracePeriodMillis, timeoutMillis)
        raftNode?.close()
        consensusProtocol?.stop()
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

    fun getPeersetId() = config.peersetId
}
