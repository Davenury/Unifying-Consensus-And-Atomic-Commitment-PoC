package com.example.api

import com.example.*
import com.example.domain.*
import com.example.raft.ChangeWithAcceptNum
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import java.io.File

class SinglePeersetIntegrationTest {

    @Test
    fun `second leader tries to become leader before first leader goes into ft-agree phase`(): Unit = runBlocking {

        deleteRaftHistories()

        val eventListener = object : EventListener {
            override fun onSignal(signal: Signal, subject: SignalSubject) {
                if (signal.addon == TestAddon.BeforeSendingAgree) {
                    expectCatching {
                        executeChange("http://localhost:8082/create_change")
                    }.isSuccess()
                }
            }
        }

        // first application - first leader; second application - second leader
        val firstLeaderAction: suspend (Transaction?) -> Unit = {
            delay(5000)
        }
        val firstLeaderCallbacks: Map<TestAddon, suspend (Transaction?) -> Unit> = mapOf(
            TestAddon.BeforeSendingAgree to firstLeaderAction
        )

        val app1 = GlobalScope.launch { startApplication(arrayOf("1", "1"), firstLeaderCallbacks, listOf(eventListener)) }
        val app2 = GlobalScope.launch { startApplication(arrayOf("2", "1"), emptyMap()) }
        val app3 = GlobalScope.launch { startApplication(arrayOf("3", "1"), emptyMap()) }

        delay(5000)

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://localhost:8081/create_change")
        }

        listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {
            deleteRaftHistories()
            val eventListener = object : EventListener {
                override fun onSignal(signal: Signal, subject: SignalSubject) {
                    if (signal.addon == TestAddon.BeforeSendingApply) {
                        expectCatching {
                            executeChange("http://localhost:8082/create_change")
                        }.isFailure()
                    }
                }
            }

            // first application - first leader; second application - second leader
            val firstLeaderAction: suspend (Transaction?) -> Unit = {
                delay(3000)
            }
            val firstLeaderCallbacks: Map<TestAddon, suspend (Transaction?) -> Unit> = mapOf(
                TestAddon.BeforeSendingApply to firstLeaderAction
            )
            val app1 = GlobalScope.launch { startApplication(arrayOf("1", "1"), firstLeaderCallbacks, listOf(eventListener)) }
            val app2 = GlobalScope.launch { startApplication(arrayOf("2", "1"), emptyMap()) }
            val app3 = GlobalScope.launch { startApplication(arrayOf("3", "1"), emptyMap()) }

            delay(5000)

            expectCatching {
                executeChange("http://localhost:8081/create_change")
            }.isSuccess()

            listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {

        deleteRaftHistories()

        runBlocking {

            lateinit var app1: Job

            val firstLeaderAction: suspend (Transaction?) -> Unit = {
                val url = "http://localhost:8082/ft-agree"
                val response = httpClient.post<Agreed>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(it!!.ballotNumber, Accept.COMMIT, changeDto)
                }
                println("Localhost 8082 sent response to ft-agree: $response")
                // kill app
                throw RuntimeException()
            }
            val firstLeaderCallbacks: Map<TestAddon, suspend (Transaction?) -> Unit> = mapOf(
                TestAddon.BeforeSendingAgree to firstLeaderAction
            )
            app1 = GlobalScope.launch { startApplication(arrayOf("1", "1"), firstLeaderCallbacks) }
            val app2: Job = GlobalScope.launch { startApplication(arrayOf("2", "1"), emptyMap()) }
            val app3: Job = GlobalScope.launch { startApplication(arrayOf("3", "1"), emptyMap()) }

            // application will start
            delay(5000)

            // change that will cause leader to fall according to action
            try {
                executeChange("http://localhost:8081/create_change")
            } catch (e: Exception) {
                println("Leader 1 fails: $e")
            }

            // leader timeout is 3 seconds for integration tests
            delay(7000)

            val response = httpClient.get<String>("http://localhost:8083/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            val change = objectMapper.readValue<ChangeWithAcceptNumDto>(response).let {
                ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
            }
            expectThat(change.change).isEqualTo(AddUserChange("userName"))

            listOf(app1, app2, app3)
                .forEach { app -> app.cancel(CancellationException()) }
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {

        deleteRaftHistories()

        val configOverrides = mapOf<String, Any>(
            "peers.peersAddresses" to listOf(createPeersInRange(4)),
            "raft.server.addresses" to listOf(List(4) { "localhost:${it + 11124}" })
        )

        val firstLeaderAction: suspend (Transaction?) -> Unit = {
            val url = "http://localhost:8082/apply"
            runBlocking {
                httpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(it!!.ballotNumber, true, Accept.COMMIT, ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "name")))
                }.also {
                    println("Got response ${it.status.value}")
                }
            }
            println("Localhost 8082 sent response to apply")
            throw RuntimeException()
        }
        val firstLeaderCallbacks: Map<TestAddon, suspend (Transaction?) -> Unit> = mapOf(
            TestAddon.BeforeSendingApply to firstLeaderAction
        )

        // when using just launch (without Global Scope) tests won't end, but we cannot use 5 apps on GlobalScope
        val app1 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "1"), firstLeaderCallbacks, configOverrides = configOverrides) }
        val app2 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "1"), emptyMap(), configOverrides = configOverrides) }
        val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap(), configOverrides = configOverrides) }
        val app4 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "1"), emptyMap(), configOverrides = configOverrides) }
        //val app5 = launch(Dispatchers.IO) { startApplication(arrayOf("5", "1"), emptyMap(), configOverrides = configOverrides) }

        // application will start
        delay(5000)

        // change that will cause leader to fall according to action
        try {
            executeChange("http://localhost:8081/create_change", mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
        } catch (e: Exception) {
            println("Leader 1 fails: $e")
        }

        // leader timeout is 5 seconds for integration tests - in the meantime third leader should execute transaction
        delay(7000)

        val response = httpClient.get<String>("http://localhost:8084/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val change = objectMapper.readValue(response, ChangeWithAcceptNumDto::class.java)
        expectThat(change.change.toChange()).isEqualTo(AddGroupChange("name"))

        // and should not execute this change couple of times
        val response2 = httpClient.get<String>("http://localhost:8082/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        println("stopping apps")
        listOf(app1, app2, app3, app4).forEach { app -> app.cancel() }

        val values: List<ChangeWithAcceptNum> = objectMapper.readValue<HistoryDto>(response2).changes.map {
            ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
        }
        println(values)
        // only one change and this change shouldn't be applied for 8082 two times
        expect {
            that(values.size).isEqualTo(1)
            that(values[0]).isEqualTo(ChangeWithAcceptNum(AddGroupChange("name"), 1))
        }
        println("here")
    }

    private fun createPeersInRange(range: Int): List<String> =
        List(range) { "localhost:${8081+it}" }

    private suspend fun executeChange(uri: String, change: Map<String, Any> = changeDto.properties) =
        httpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private val changeDto = ChangeDto(
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }
}