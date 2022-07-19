package com.github.davenury.ucac

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.consensus.ratis.HistoryRaftNode
import com.github.davenury.ucac.consensus.ratis.RaftConfiguration
import com.github.davenury.ucac.consensus.ratis.RatisHistoryManagement
import com.github.davenury.ucac.consensus.ratis.ratisRouting
import com.github.davenury.ucac.gpac.api.gpacProtocolRouting
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocolImpl
import com.github.davenury.ucac.gpac.domain.ProtocolClientImpl
import com.github.davenury.ucac.gpac.domain.TransactionBlockerImpl
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
import java.time.Duration

fun main(args: Array<String>) {
    createApplication(args).startBlocking()
}

fun createApplication(
    args: Array<String>,
    signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap(),
    mode: ApplicationMode = LocalDevelopmentApplicationMode(args)
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
    private lateinit var ctx: ExecutorCoroutineDispatcher
    private lateinit var gpacProtocol: GPACProtocol
    private lateinit var consensusProtocol: RaftConsensusProtocol

    init {
        engine = embeddedServer(Netty, port = mode.port, host = "0.0.0.0") {
            raftNode = HistoryRaftNode(mode.nodeId, mode.peersetId, peerConstants)

            ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

            consensusProtocol = RaftConsensusProtocolImpl(
                mode.nodeId,
                mode.peersetId,
                ProtocolTimerImpl(Duration.ZERO, Duration.ofSeconds(1), ctx), // TODO move to config
                if (mode is TestApplicationMode) listOf() else mode.otherPeers[mode.peersetId - 1]
            )

            val historyManagement = RatisHistoryManagement(raftNode!!)
//            val historyManagement = InMemoryHistoryManagement(consensusProtocol)
            val signalPublisher = SignalPublisher(signalListeners)
            val timer = ProtocolTimerImpl(config.protocol.leaderFailTimeout, config.protocol.backoffBound, ctx)
            val protocolClient = ProtocolClientImpl()
            val transactionBlocker = TransactionBlockerImpl()
            gpacProtocol =
                GPACProtocolImpl(
                    historyManagement,
                    config.peers.maxLeaderElectionTries,
                    timer,
                    protocolClient,
                    transactionBlocker,
                    mode.otherPeers,
                    signalPublisher,
                    mode.port,
                    mode.peersetId
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

            commonRouting(gpacProtocol, consensusProtocol as RaftConsensusProtocolImpl)
            ratisRouting(historyManagement)
            gpacProtocolRouting(gpacProtocol)
            consensusProtocolRouting(consensusProtocol)

            if (mode !is TestApplicationMode) {
    //        runBlocking {
    //            startConsensusProtocol()
    //        }
            }
        }
    }

    fun setOtherPeers(otherPeers: List<List<String>>) {
        gpacProtocol.setOtherPeers(otherPeers)
        consensusProtocol.setOtherPeers(otherPeers[mode.peersetId - 1])
    }

    fun startBlocking() {
        engine.start(wait = true)
    }

    suspend fun startConsensusProtocol() {
        consensusProtocol.begin()
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

    fun getPeersetId() = mode.peersetId
}

