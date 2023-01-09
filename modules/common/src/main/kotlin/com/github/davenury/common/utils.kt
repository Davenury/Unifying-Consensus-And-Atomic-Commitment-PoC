package com.github.davenury.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
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
    fun bumpIncorrectHistory() {
        meterRegistry.counter("incorrect_history_change").increment()
    }

    fun bumpChangeProcessed(changeResult: ChangeResult) {
        Counter
            .builder("change_processed")
            .tag("result", changeResult.status.name.lowercase())
            .register(meterRegistry)
            .increment()
    }

    fun startTimer(changeId: String) {
        changeIdToTimer[changeId] = Instant.now()
    }
    fun stopTimer(changeId: String, protocol: String, result: ChangeResult) {
        val timeElapsed = Duration.between(changeIdToTimer[changeId]!!, Instant.now())
        Timer
            .builder("change_processing_time")
            .tag("protocol", protocol)
            .tag("result", result.status.name.lowercase())
            .register(meterRegistry)
            .record(timeElapsed)
    }
}
