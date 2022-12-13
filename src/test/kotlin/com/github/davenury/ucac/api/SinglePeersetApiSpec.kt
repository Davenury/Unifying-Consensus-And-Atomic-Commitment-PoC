package com.github.davenury.ucac.api


import com.github.davenury.common.AddUserChange
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.objectMapper
import com.github.davenury.ucac.utils.ApplicationTestcontainersEnvironment
import com.github.davenury.ucac.utils.TestLogExtension
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
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * @author Kamil Jarosz
 */
@Disabled
@Testcontainers
@ExtendWith(TestLogExtension::class)
class SinglePeersetApiSpec {
    companion object {
        private val logger = LoggerFactory.getLogger(SinglePeersetApiSpec::class.java)
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
            listOf(
                ChangePeersetInfo(0, InitialHistoryEntry.getId())
            ),
            "test user",
        )

        logger.info("Sending change $change")

        val peer0Address = environment.getAddress(0, 0)
        val response = http.post<HttpResponse>("http://${peer0Address}/v2/change/sync") {
            contentType(ContentType.Application.Json)
            body = change
        }
        TODO("finish this test")
    }
}
