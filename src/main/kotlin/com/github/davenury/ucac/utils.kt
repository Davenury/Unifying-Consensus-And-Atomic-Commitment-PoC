package com.github.davenury.ucac

import com.github.davenury.common.objectMapper
import com.zopa.ktor.opentracing.OpenTracingClient
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*

val httpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 5_000
    }
    install(OpenTracingClient)
}
val raftHttpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        connectTimeoutMillis = 125
        requestTimeoutMillis = 750
    }
    install(OpenTracingClient)
}

val raftHttpClients = (0..5).map {
    HttpClient(OkHttp) {
        install(JsonFeature) {
            serializer = JacksonSerializer(objectMapper)
        }
        install(HttpTimeout) {
            connectTimeoutMillis = 125
            requestTimeoutMillis = 750
        }
        install(OpenTracingClient)
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
