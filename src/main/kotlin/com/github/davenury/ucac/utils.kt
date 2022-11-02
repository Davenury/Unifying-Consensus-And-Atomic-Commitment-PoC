package com.github.davenury.ucac

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import java.security.MessageDigest


val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
val httpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 2_000
    }
}
val raftHttpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 500
    }
}
val testHttpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 15000
        socketTimeoutMillis = 120000
    }
}

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
