package com.example.api

import com.example.*
import com.example.domain.*
import com.example.raft.ChangeWithAcceptNum
import com.example.utils.eventually
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.client.utils.EmptyContent.contentType
import io.ktor.http.*
import io.ktor.server.netty.*
import kotlinx.coroutines.*
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isNotNull
import strikt.assertions.isSuccess
import java.io.File
import java.io.FileFilter

class IntegrationTest {

    @Test
    fun `should react to event`(): Unit = runBlocking {

        val receivedSignals = mutableListOf<TestAddon>()
        val eventListener = object : EventListener {
            override fun onSignal(signal: Signal, subject: SignalSubject) {
                if (signal.addon == TestAddon.OnHandlingApplyEnd)
                    receivedSignals.add(signal.addon)
            }
        }

        // given - leader application
        val sniffedTransactions = mutableListOf<Transaction?>()
        val transactionSniffer: suspend (Transaction?) -> Unit = { transaction -> sniffedTransactions.add(transaction) }
        val app1 = GlobalScope.launch {
            startApplication(
                arrayOf("1"),
                mapOf(TestAddon.BeforeSendingElect to transactionSniffer),
                listOf(eventListener)
            )
        }
        val app2 = GlobalScope.launch { startApplication(arrayOf("2"), emptyMap()) }
        val app3 = GlobalScope.launch { startApplication(arrayOf("3"), emptyMap()) }

        delay(5000)

        httpClient.post<String>("http://0.0.0.0:8081/create_change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = changeDto.properties
        }

        expect {
            that(sniffedTransactions.size).isEqualTo(1)
            that(sniffedTransactions[0]).isEqualTo(Transaction(1, Accept.COMMIT))
            that(receivedSignals.size).isEqualTo(1)
            that(receivedSignals[0]).isEqualTo(TestAddon.OnHandlingApplyEnd)
        }

        listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
    }

    @Test
    fun `second leader tries to become leader before first leader goes into ft-agree phase`(): Unit = runBlocking {

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

            val map = objectMapper.readValue(response, HashMap<String, Any>().javaClass)
            val changeString = objectMapper.writeValueAsString(map["change"])
            val change: Change = Change.fromJson(changeString)
            expectThat(change).isEqualTo(AddUserChange("userName"))

            listOf(app1, app2, app3)
                .forEach { app -> app.cancel(CancellationException()) }
        }
    }

    //buggy for now
//    @Test
//    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
//        val firstLeaderAction: suspend (Transaction?) -> Unit = {
//            val url = "http://localhost:8082/apply"
//            val response = httpClient.post<HttpStatement>(url) {
//                contentType(ContentType.Application.Json)
//                accept(ContentType.Application.Json)
//                body = Apply(it!!.ballotNumber, true, Accept.COMMIT, changeDto)
//            }
//            println("Localhost 8082 sent response to apply: $response")
//            throw KillApplicationException()
//        }
//        val firstLeaderCallbacks: Map<TestAddon, suspend (Transaction?) -> Unit> = mapOf(
//            TestAddon.BeforeSendingApply to firstLeaderAction
//        )
//        val app1 = GlobalScope.launch { startApplication(arrayOf("1", "1"), firstLeaderCallbacks) }
//        val app2 = GlobalScope.launch { startApplication(arrayOf("2", "1"), emptyMap()) }
//        val app3 = GlobalScope.launch { startApplication(arrayOf("3", "1"), emptyMap()) }
//
//        // application will start
//        delay(5000)
//
//        // change that will cause leader to fall according to action
//        try {
//            executeChange("http://localhost:8081/create_change")
//        } catch (e: Exception) {
//            println("Leader 1 fails: $e")
//        }
//
//        // leader timeout is 3 seconds for integration tests
//        delay(7000)
//
//        val response = httpClient.get<String>("http://localhost:8082/change") {
//            contentType(ContentType.Application.Json)
//            accept(ContentType.Application.Json)
//        }
//
//        val map = objectMapper.readValue(response, HashMap<String, Any>().javaClass)
//        val changeString = objectMapper.writeValueAsString(map["change"])
//        val change: Change = Change.fromJson(changeString)
//        expectThat(change).isEqualTo(AddUserChange("userName"))
//
//        // and should not execute this change couple of times
//        val response2 = httpClient.get<String>("http://localhost:8082/changes") {
//            contentType(ContentType.Application.Json)
//            accept(ContentType.Application.Json)
//        }
//        println("here: $response2")
////        val map = objectMapper.readValue(response, HashMap<String, Any>().javaClass)
////        val changeString = objectMapper.writeValueAsString(map["change"])
////        val change: Change = Change.fromJson(changeString)
////        expectThat(change).isEqualTo(AddUserChange("userName"))
//
//        listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
//    }

    private suspend fun executeChange(uri: String) =
        httpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = changeDto.properties
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