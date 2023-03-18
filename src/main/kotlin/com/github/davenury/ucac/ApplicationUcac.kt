package com.github.davenury.ucac

import com.github.davenury.common.*
import com.github.davenury.common.history.historyRouting
import com.github.davenury.ucac.api.ApiV2Service
import com.github.davenury.ucac.api.apiV2Routing
import com.github.davenury.ucac.commitment.gpac.GPACChannels
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.gpac.GPACQueue
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.commitment.twopc.TwoPCProtocolClientImpl
import com.github.davenury.common.GlobalPeerId
import com.github.davenury.ucac.common.HistoryFactory
import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.common.TransactionBlocker
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.routing.consensusProtocolRouting
import com.github.davenury.ucac.routing.gpacProtocolRouting
import com.github.davenury.ucac.routing.metaRouting
import com.github.davenury.ucac.routing.twoPCRouting
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
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.slf4j.event.Level
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

    ApplicationUcac.logger.info("Using overrides: $configOverrides")

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

fun createApplication(
    signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap(),
): ApplicationUcac {
    val config = loadConfig<Config>(configOverrides)
    return ApplicationUcac(signalListeners, config)
}

class ApplicationUcac constructor(
    private val signalListeners: Map<Signal, SignalListener> = emptyMap(),
    private val config: Config,
    inheritMdc: Boolean = true,
) {
    private val mdc: MutableMap<String, String> = HashMap(mapOf("peer" to config.globalPeerId().toString()))
    private val engine: NettyApplicationEngine
    private var consensusProtocol: RaftConsensusProtocol? = null
    private var twoPC: TwoPC? = null
    private val ctx: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private lateinit var gpacFactory: GPACFactory
    private var service: ApiV2Service? = null
    private val peerResolver = config.newPeerResolver()

    init {
        if (inheritMdc) {
            MDC.getCopyOfContextMap()?.let { mdc.putAll(it) }
        }
    }

    private fun <R> withMdc(action: () -> R): R {
        val oldMdc = MDC.getCopyOfContextMap() ?: HashMap()
        mdc.forEach { MDC.put(it.key, it.value) }
        try {
            return action()
        } finally {
            MDC.setContextMap(oldMdc)
        }
    }

    init {
        var engine: NettyApplicationEngine? = null
        withMdc {
            logger.info("Starting application with config: $config")
            engine = createServer()
        }
        this.engine = engine!!
    }

    private fun createServer() = embeddedServer(Netty, port = config.port, host = "0.0.0.0") {
        val history = HistoryFactory().createForConfig(config)
        val signalPublisher = SignalPublisher(signalListeners)

        val raftProtocolClientImpl = RaftProtocolClientImpl()

        val transactionBlocker = TransactionBlocker()
        val gpacChannels = GPACChannels.create()
        val gpacQueue = GPACQueue(gpacChannels)
        gpacFactory = GPACFactory(transactionBlocker, history, config, ctx, signalPublisher, peerResolver, gpacChannels)

        consensusProtocol = RaftConsensusProtocolImpl(
            history,
            config.host + ":" + config.port,
            ctx,
            peerResolver,
            signalPublisher,
            raftProtocolClientImpl,
            heartbeatTimeout = config.raft.heartbeatTimeout,
            heartbeatDelay = config.raft.leaderTimeout,
            transactionBlocker = transactionBlocker,
            config.metricTest
        )

        twoPC = TwoPC(
            history,
            config.twoPC,
            ctx,
            TwoPCProtocolClientImpl(config.peerId),
            consensusProtocol as RaftConsensusProtocolImpl,
            signalPublisher,
            peerResolver,
            config.metricTest
        )

        service = ApiV2Service(
            gpacFactory,
            consensusProtocol as RaftConsensusProtocolImpl,
            twoPC!!,
            history,
            config,
            peerResolver
        )

        install(CallLogging) {
            level = Level.DEBUG
            mdc.forEach { mdcEntry ->
                mdc(mdcEntry.key) { mdcEntry.value }
            }
        }

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
            exception<HistoryCannotBeBuildException> {
                call.respond(
                    HttpStatusCode.BadRequest,
                    ErrorMessage(
                        "Change you're trying to perform is not applicable with current state"
                    )
                )
            }
            exception<AlreadyLockedException> { cause ->
                call.respond(
                    HttpStatusCode.Conflict,
                    ErrorMessage(
                        cause.message!!
                    )
                )
            }

            exception<ChangeDoesntExist> { cause ->
                logger.error("Change doesn't exist")
                call.respond(
                    status = HttpStatusCode.NotFound,
                    ErrorMessage(cause.message ?: "Change doesn't exists")
                )
            }

            exception<TwoPCHandleException> { cause ->
                call.respond(
                    status = HttpStatusCode.BadRequest,
                    ErrorMessage(
                        cause.message!!
                    )
                )
            }

            exception<GPACInstanceNotFoundException> { cause ->
                logger.error("GPAC instance not found")
                call.respond(
                    status = HttpStatusCode.NotFound,
                    ErrorMessage(cause.message ?: "GPAC instance not found")
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
        apiV2Routing(service!!, peerResolver.currentPeer())
        gpacProtocolRouting(gpacQueue)
        consensusProtocolRouting(consensusProtocol!!, peerResolver)
        twoPCRouting(twoPC!!)

        runBlocking {
            if (config.raft.isEnabled) consensusProtocol!!.begin()
            gpacFactory.startProcessing()
        }
    }

    fun setPeers(peers: Map<GlobalPeerId, PeerAddress>) {
        withMdc {
            val oldPeers = peerResolver.getPeersPrintable()
            peerResolver.setPeers(peers)
            val newPeers = peerResolver.getPeersPrintable()
            logger.info("Set peers $oldPeers -> $newPeers")
        }
    }

    fun startNonblocking() {
        withMdc {
            engine.start(wait = false)
            val address = "${config.host}:${getBoundPort()}"
            consensusProtocol?.setPeerAddress(address)
        }
    }

    fun stop(gracePeriodMillis: Long = 200, timeoutMillis: Long = 1000) {
        withMdc {
            ctx.close()
            engine.stop(gracePeriodMillis, timeoutMillis)
            consensusProtocol?.stop()
        }
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

    fun getGlobalPeerId() = config.globalPeerId()

    companion object {
        val logger: Logger = LoggerFactory.getLogger("ucac")
    }
}
