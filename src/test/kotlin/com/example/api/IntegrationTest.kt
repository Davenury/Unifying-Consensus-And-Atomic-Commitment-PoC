package com.example.api

import com.example.*
import com.example.domain.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.eclipse.jetty.server.Server
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isEqualTo
import java.net.ServerSocket

class IntegrationTest {

    @Test
    fun `should react to event`(): Unit = runBlocking {

        // given - leader application
        val sniffedTransactions = mutableListOf<Transaction?>()
        val transactionSniffer: (Transaction?) -> Unit = { transaction -> sniffedTransactions.add(transaction) }
        val app1 = GlobalScope.launch { startApplication(arrayOf("1"), mapOf(TestAddon.OnSendingElect to transactionSniffer)) }
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
            that(sniffedTransactions[0]).isEqualTo(Transaction(0, Accept.COMMIT))
        }

        listOf(app1, app2, app3).forEach { app -> app.cancel(CancellationException("Test is over")) }
    }

    @Test
    // This test just checks if we should cancel coroutines after each tests or will they cancel by themselves
    fun `should just start with another leader application`(): Unit = runBlocking {
        GlobalScope.launch { startApplication(arrayOf("1"), emptyMap()) }
    }

    private val changeDto = ChangeDto(mapOf(
        "operation" to "ADD_USER",
        "userName" to "userName"
    ))

}