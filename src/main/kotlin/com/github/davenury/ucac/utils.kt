package com.github.davenury.ucac

import com.github.davenury.common.objectMapper
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*

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
