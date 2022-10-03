package com.github.davenury.ucac

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.security.MessageDigest

val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
val httpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 2000
    }
}
val testHttpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 15000
    }
}

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}
