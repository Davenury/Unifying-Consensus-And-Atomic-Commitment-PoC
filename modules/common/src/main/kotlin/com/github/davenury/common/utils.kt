package com.github.davenury.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import java.security.MessageDigest


val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
