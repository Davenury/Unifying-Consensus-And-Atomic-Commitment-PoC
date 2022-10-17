package com.github.davenury.ucac

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.historyManagementRouting
import com.github.davenury.ucac.consensus.raft.api.consensusProtocolRouting
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.consensus.ratis.HistoryRaftNode
import com.github.davenury.ucac.consensus.ratis.RaftConfiguration
import com.github.davenury.ucac.gpac.api.gpacProtocolRouting
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocolClientImpl
import com.github.davenury.ucac.gpac.domain.GPACProtocolImpl
import com.github.davenury.ucac.gpac.domain.TransactionBlockerImpl
import com.github.davenury.ucac.history.History
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
import java.util.concurrent.Executors
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

fun main(args: Array<String>) {
    createApplication(args).startBlocking()
}

fun createApplication(
    args: Array<String>,
    signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap(),
    mode: ApplicationMode = determineApplicationMode(args)
): Application {
    return Application(signalListeners, configOverrides, mode)
}

class Application constructor(
    private val signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any>,
    private val mode: ApplicationMode
) {
    private val config: Config = loadConfig(configOverrides)
    private val peerConstants: RaftConfiguration = RaftConfiguration(mode.peersetId, configOverrides)
    private val engine: NettyApplicationEngine
    private var raftNode: HistoryRaftNode? = null
    private var consensusProtocol: RaftConsensusProtocol? = null
    private val ctx: ExecutorCoroutineDispatcher
    private lateinit var gpacProtocol: GPACProtocol

    init {

        ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
        engine = embeddedServer(Netty, port = mode.port, host = "0.0.0.0") {

            val signalPublisher = SignalPublisher(signalListeners)

            val raftProtocolClientImpl = RaftProtocolClientImpl(mode.nodeId)

            consensusProtocol = RaftConsensusProtocolImpl(
                mode.nodeId,
                mode.peersetId,
                mode.host,
                ctx,
                mode.otherPeers.getOrElse(mode.peersetId - 1) { listOf() },
                signalPublisher,
                raftProtocolClientImpl,
                heartbeatTimeout = config.raft.heartbeatTimeout,
                heartbeatDelay = config.raft.leaderTimeout
            )

            val historyManagement = InMemoryHistoryManagement(consensusProtocol as ConsensusProtocol<Change, History>)

            val timer = ProtocolTimerImpl(config.protocol.leaderFailTimeout, config.protocol.backoffBound, ctx)
            val protocolClient = GPACProtocolClientImpl()
            val transactionBlocker = TransactionBlockerImpl()
            val myAddress = "${mode.host}:${mode.port}"
            gpacProtocol =
                GPACProtocolImpl(
                    historyManagement,
                    config.peers.maxLeaderElectionTries,
                    timer,
                    protocolClient,
                    transactionBlocker,
                    signalPublisher,
                    allPeers = mode.otherPeers.withIndex().associate { it.index + 1 to it.value },
                    myPeersetId = mode.peersetId,
                    myNodeId = mode.nodeId,
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

            commonRouting(gpacProtocol, consensusProtocol as RaftConsensusProtocolImpl)
            historyManagementRouting(historyManagement)
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
        consensusProtocol?.setOtherPeers(peers[mode.peersetId]!!)
    }

    fun startBlocking() {
        engine.start(wait = true)
    }

    suspend fun startConsensusProtocol() {
        consensusProtocol?.begin()
    }

    fun startNonblocking() {
        engine.start(wait = false)
        val address = "${mode.host}:${getBoundPort()}"
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

    fun getPeersetId() = mode.peersetId
}

