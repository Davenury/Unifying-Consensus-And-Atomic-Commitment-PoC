package com.example.api

import com.example.*
import com.example.domain.Accept
import com.example.domain.ChangeDto
import com.example.domain.Transaction
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess

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
    fun `second leader comes with its transaction before first leader goes into ft-agree phase`(): Unit = runBlocking {

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

        val app1 = GlobalScope.launch { startApplication(arrayOf("1"), firstLeaderCallbacks, listOf(eventListener)) }
        val app2 = GlobalScope.launch { startApplication(arrayOf("2"), emptyMap()) }
        val app3 = GlobalScope.launch { startApplication(arrayOf("3"), emptyMap()) }

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
            val app1 = GlobalScope.launch { startApplication(arrayOf("1"), firstLeaderCallbacks, listOf(eventListener)) }
            val app2 = GlobalScope.launch { startApplication(arrayOf("2"), emptyMap()) }
            val app3 = GlobalScope.launch { startApplication(arrayOf("3"), emptyMap()) }

            delay(5000)

            expectCatching {
                executeChange("http://localhost:8081/create_change")
            }.isSuccess()

            listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
        }

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
}