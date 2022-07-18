package com.example.consensus

import com.example.common.AddUserChange
import com.example.consensus.ratis.ChangeWithAcceptNum
import com.example.consensus.ratis.HistoryDto
import com.example.objectMapper
import com.example.startApplication
import com.example.testHttpClient
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

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

        val peer1 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "1")) }
        val peer2 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "1")) }
        val peer3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1")) }
        val peer4 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "1")) }
        val peer5 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("5", "1")) }

        delay(5000)

        // when: peer1 executed change
        expectCatching {
            executeChange("$peer1/consensus/create_change")
        }.isSuccess()

        val changes = askForChanges("http://localhost:8083")

        // then: there's one change and it's change we've requested
        expect {
            that(changes.size).isEqualTo(1)
            that(changes[0].change).isEqualTo(AddUserChange("userName"))
        }

        // when: peer2 executes change
        expectCatching {
            executeChange("$peer2/consensus/create_change")
        }.isSuccess()

        val changes2 = askForChanges("http://localhost:8083")

        // then: there are two changes
        expect {
            that(changes2.size).isEqualTo(2)
            that(changes2[1].change).isEqualTo(AddUserChange("userName"))
        }

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
        testHttpClient.get<String>("$peer/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

}
