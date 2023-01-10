package com.github.davenury.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import java.security.MessageDigest
import java.util.*


val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
object Metrics {

    private val changeToTimer: MutableMap<String, LongTaskTimer.Sample> = mutableMapOf()
    fun bumpIncorrectHistory() {
        meterRegistry.counter("incorrect_history_change").increment()
    }

    fun startTimer(changeId: String, protocol: String) {
        changeToTimer[changeId] = LongTaskTimer
            .builder("change_processing_time")
            .tag("protocol", protocol)
            .register(meterRegistry).start()
    }

    fun stopTimer(changeId: String) {
        changeToTimer[changeId]?.stop()
    }
}
