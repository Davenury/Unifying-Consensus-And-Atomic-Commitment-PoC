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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration

fun main() {
    val service = TestNotificationService()
    service.startService()
}

class TestNotificationService {

    private val config = loadConfig<Config>(decoders = listOf(StrategyDecoder()))

    private val peers = config.peerAddresses()
    private val changes = Changes(peers, HttpSender(config.notificationServiceAddress), config.getStrategy())
    private val testExecutor = TestExecutor(
        config.numberOfRequestsToSendToSinglePeerset,
        config.numberOfRequestsToSendToMultiplePeersets,
        config.durationOfTest,
        config.maxPeersetsInChange,
        changes
    )

    init {
        println("Pushgateway address: ${config.pushGatewayAddress}")
    }

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
                    println(e)
                    call.respond(HttpStatusCode.ServiceUnavailable)
                }
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
        pushGateway.push(meterRegistry.prometheusRegistry, "test_service")
        server.stop(200, 1000)
    }

}
