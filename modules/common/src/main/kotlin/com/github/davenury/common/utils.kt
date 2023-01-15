package com.github.davenury.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit


val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
object Metrics {

    private val changeIdToTimer: MutableMap<String, Instant> = mutableMapOf()

    private var lastHearbeat: Instant = Instant.now()
    fun bumpIncorrectHistory() {
        meterRegistry.counter("incorrect_history_change").increment()
    }

    fun bumpChangeProcessed(changeResult: ChangeResult, protocol: String) {
        Counter
            .builder("change_processed")
            .tag("result", changeResult.status.name.lowercase())
            .tag("protocol", protocol)
            .register(meterRegistry)
            .increment()
    }

    fun startTimer(changeId: String) {
        changeIdToTimer[changeId] = Instant.now()
    }
    fun stopTimer(changeId: String, protocol: String, result: ChangeResult) {
        val timeElapsed = Duration.between(changeIdToTimer[changeId]!!, Instant.now())
        logger.info("Time elapsed for change: $changeId: $timeElapsed")
        Timer
            .builder("change_processing_time")
            .tag("protocol", protocol)
            .tag("result", result.status.name.lowercase())
            .register(meterRegistry)
            .record(timeElapsed)
    }
    
    fun refreshLastHeartbeat() {
        lastHearbeat = Instant.now()
    }

    fun registerTimerHeartbeat(){
        val now = Instant.now()
        Timer
            .builder("heartbeat_processing_time")
            .register(meterRegistry)
            .record(Duration.between(lastHearbeat, now))
        lastHearbeat = now
    }

    private val logger = LoggerFactory.getLogger("Metrics")
}
