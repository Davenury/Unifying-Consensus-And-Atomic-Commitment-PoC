package com.github.davenury.ucac

import com.github.davenury.common.objectMapper
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import okhttp3.ConnectionPool
import java.util.concurrent.TimeUnit

val httpClient = HttpClient(OkHttp) {
    install(JsonFeature) {
        serializer = JacksonSerializer(objectMapper)
    }
    install(HttpTimeout) {
        requestTimeoutMillis = 5_000
    }
}
fun raftHttpClient() = HttpClient(OkHttp) {
    engine {
        this.config {
            this.connectionPool(ConnectionPool(0, 1, TimeUnit.MILLISECONDS))
            this.cache(null)
        }
    }
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
