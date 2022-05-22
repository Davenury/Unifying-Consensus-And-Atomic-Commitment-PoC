package com.example.api

import com.example.GPACEventData
import com.example.domain.ChangeDto
import com.example.domain.ElectMe
import com.example.domain.ElectedYou
import com.example.httpClient
import com.example.startApplication
import com.example.subscribeToEvent
import io.ktor.client.request.*
import io.ktor.client.utils.EmptyContent.contentType
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class EventListenerTest {

    companion object {

        private val events = mutableListOf<GPACEventData>()

        @BeforeAll
        @JvmStatic
        fun setup() {
            GlobalScope.launch { startApplication(arrayOf("1"), listOf { application -> application.subscribeToEvent { events.add(it) } }) }
            GlobalScope.launch { startApplication(arrayOf("2"), emptyList()) }
            GlobalScope.launch { startApplication(arrayOf("3"), emptyList()) }
        }
    }

    @Test
    fun `should react to event`(): Unit = runBlocking {

        delay(5000)

        httpClient.post<ElectedYou>("http://0.0.0.0:8081/elect") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = ElectMe(10, changeDto)
        }

        delay(100)

        expectThat(events.size).isEqualTo(1)

    }

    private val changeDto = ChangeDto(mapOf(
        "operation" to "ADD_USER",
        "userName" to "userName"
    ))

}