package com.github.davenury.tests

import com.github.davenury.common.Notification
import com.github.davenury.common.loadConfig
import com.github.davenury.common.meterRegistry
import com.github.davenury.common.objectMapper
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.metrics.micrometer.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.prometheus.client.exporter.PushGateway
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

fun main() {
    val service = TestNotificationService()
    service.startService()
}

class TestNotificationService {

    private val config = loadConfig<Config>(decoders = listOf(StrategyDecoder(), ACProtocolDecoder()))

    init {
        logger.info("Starting performance tests with config: $config")
    }

    private val peers = config.peerAddresses()
    private val changes = Changes(peers, HttpSender(config.acProtocol), config.getStrategy(), config.notificationServiceAddress)
    private val testExecutor = TestExecutor(
        config.numberOfRequestsToSendToSinglePeerset,
        config.numberOfRequestsToSendToMultiplePeersets,
        config.durationOfTest,
        config.maxPeersetsInChange,
        changes,
        config.constantLoad,
        config.fixedPeersetsInChange
    )

    private val server: NettyApplicationEngine = embeddedServer(Netty, port=8080, host = "0.0.0.0") {
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

        routing {
            post("/api/v1/notification") {
                try {
                    val notification = call.receive<Notification>()
                    changes.handleNotification(notification)
                    call.respond(HttpStatusCode.OK)
                } catch (e: Exception) {
                    logger.error("Error while handling notification", e)
                    call.respond(HttpStatusCode.ServiceUnavailable)
                }
            }

            get("/_meta/metrics") {
                call.respond(meterRegistry.scrape())
            }
        }

        GlobalScope.launch {
            delay(2000)
            testExecutor.startTest()
            delay(2000)
            closeService()
        }
    }

    fun startService() {
        server.start(wait = true)
    }

    fun closeService() {
        val pushGateway = PushGateway(config.pushGatewayAddress)
        pushGateway.pushAdd(meterRegistry.prometheusRegistry, "test_service")
        server.stop(200, 1000)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestNotificationService")
    }

}
