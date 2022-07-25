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
        System.setProperty("configFile", "consensus_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {

        //1. happy-path, wszyscy żyją i jeden zostaje wybrany jako leader
        //* peer 1 wysyła prośbę o głosowanie na niego
        //* peer 1 dostaje większość głosów
        //* peer 1 informuje że jest leaderem
        //* peer 1 proponuje zmianę (akceptowana)
        //* peer 2 proponuje zmianę (akceptowana)

        val peer1 = createApplication(arrayOf("1", "1"))
        val peer2 = createApplication(arrayOf("2", "1"))
        val peer3 = createApplication(arrayOf("3", "1"))
        val peer4 = createApplication(arrayOf("4", "1"))
        val peer5 = createApplication(arrayOf("5", "1"))

        val peers = listOf(peer1,peer2,peer3,peer4,peer5)
        peers.forEach { it.startNonblocking() }

        val propagationDelay = 7_000L


        delay(propagationDelay)
        val peer1Address = "http://localhost:8081"
        val peer2Address = "http://localhost:8082"

        // when: peer1 executed change
        expectCatching {
            executeChange("$peer1Address/consensus/create_change", createChangeWithAcceptNum(null))
        }.isSuccess()

        delay(propagationDelay)

        val changes = askForChanges("http://localhost:8083")

        // then: there's one change and it's change we've requested
        expect {
            that(changes.size).isEqualTo(1)
            that(changes[0].change).isEqualTo(AddUserChange("userName"))
            that(changes[0].acceptNum).isEqualTo(null)
        }

        // when: peer2 executes change
        expectCatching {
            executeChange("$peer2Address/consensus/create_change", createChangeWithAcceptNum(1))
        }.isSuccess()

        delay(propagationDelay)

        val changes2 = askForChanges("http://localhost:8083")

        // then: there are two changes
        expect {
            that(changes2.size).isEqualTo(2)
            that(changes2[1].change).isEqualTo(AddUserChange("userName"))
            that(changes2[1].acceptNum).isEqualTo(1)
        }

        peers.forEach { it.stop() }

    }

    private val change =
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )


    private fun createChangeWithAcceptNum(acceptNum: Int?) = mapOf(
        "change" to change,
        "acceptNum" to acceptNum
    )


    private suspend fun executeChange(uri: String, requestBody: Map<String, Any?>) =
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

