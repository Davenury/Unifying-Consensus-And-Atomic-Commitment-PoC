package com.github.davenury.ucac.api


import com.github.davenury.common.AddUserChange
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.objectMapper
import com.github.davenury.ucac.utils.ApplicationTestcontainersEnvironment
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * @author Kamil Jarosz
 */
@Disabled
@Testcontainers
class SinglePeersetApiSpec {
    companion object {
        private val log = LoggerFactory.getLogger(SinglePeersetApiSpec::class.java)
    }

    private val http = HttpClient(OkHttp) {
        install(JsonFeature) {
            serializer = JacksonSerializer(objectMapper)
        }
        install(HttpTimeout) {
            socketTimeoutMillis = 120000
        }
    }

    @Container
    private val environment = ApplicationTestcontainersEnvironment(listOf(3))

    @Test
    fun `sync api`(): Unit = runBlocking {
        val change = AddUserChange(
            InitialHistoryEntry.getId(),
            "test user",
            listOf(),
        )

        log.info("Sending change $change")

        val peer0Address = environment.getAddress(1, 1)
        val response = http.post<HttpResponse>("http://${peer0Address}/v2/change/sync") {
            contentType(ContentType.Application.Json)
            body = change
        }
        TODO("finish this test")
    }
}
