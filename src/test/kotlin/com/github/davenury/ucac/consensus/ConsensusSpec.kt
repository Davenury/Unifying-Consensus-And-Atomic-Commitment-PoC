package com.github.davenury.ucac.consensus

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.ratis.HistoryDto
import com.github.davenury.ucac.createApplication
import com.github.davenury.ucac.objectMapper
import com.github.davenury.ucac.testHttpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

@Disabled("WIP")
class ConsensusSpec {

    @BeforeEach
    internal fun setUp() {
        System.setProperty("configFile", "single_peerset_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {

        //1. happy-path, wszyscy żyją i jeden zostaje wybrany jako leader
        //* peer 1 wysyła prośbę o głosowanie na niego
        //* peer 1 dostaje większość głosów
        //* peer 1 informuje że jest leaderem
        //* peer 1 proponuje zmianę (akceptowana)
        //* peer 2 proponuje zmianę (akceptowana)

        val app1 = createApplication(arrayOf("1", "1"))
        val app2 = createApplication(arrayOf("2", "1"))
        val app3 = createApplication(arrayOf("3", "1"))
        val app4 = createApplication(arrayOf("4", "1"))
        val app5 = createApplication(arrayOf("5", "1"))
        val apps = listOf(app1, app2, app3, app4, app5)
        apps.forEach { app -> app.startNonblocking() }

        val propagationDelay = 8_000L


        delay(propagationDelay)
        val peer1Address = "http://localhost:8081"
        val peer2Address = "http://localhost:8082"

        // when: peer1 executed change
        expectCatching {
            executeChange("$peer1Address/consensus/create_change")
        }.isSuccess()

        delay(propagationDelay)

        val changes = askForChanges("http://localhost:8083")

        // then: there's one change and it's change we've requested
        expect {
            that(changes.size).isEqualTo(1)
            that(changes[0].change).isEqualTo(AddUserChange("userName"))
        }

        // when: peer2 executes change
        expectCatching {
            executeChange("$peer2Address/consensus/create_change")
        }.isSuccess()

        delay(propagationDelay)

        val changes2 = askForChanges("http://localhost:8083")

        // then: there are two changes
        expect {
            that(changes2.size).isEqualTo(2)
            that(changes2[1].change).isEqualTo(AddUserChange("userName"))
        }

        apps.forEach { app -> app.stop() }
    }

    private val change = mapOf(
        "operation" to "ADD_USER",
        "userName" to "userName"
    )

    private suspend fun executeChange(uri: String, requestBody: Map<String, Any> = change) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = requestBody
        }

    private suspend fun askForChanges(peer: String) =
        testHttpClient.get<String>("$peer/consensus/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.changeDto.toChange(), it.acceptNum) }

    private val peer1 = "http://localhost:8081"
    private val peer2 = "http://localhost:8082"
}

