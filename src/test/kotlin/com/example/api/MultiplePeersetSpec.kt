package com.example.api

import com.example.domain.AddUserChange
import com.example.domain.ChangeDto
import com.example.httpClient
import com.example.objectMapper
import com.example.raft.ChangeWithAcceptNum
import com.example.startApplication
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.isEqualTo
import java.io.File

class MultiplePeersetSpec {

    @BeforeEach
    fun setup() {
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {

        // Constants are shared by multiple threads as those are not indeed instances of different applications
        // given - applications
        val app1 = GlobalScope.launch { startApplication(arrayOf("1", "1"), emptyMap()) }
        val app2 = GlobalScope.launch { startApplication(arrayOf("2", "1"), emptyMap()) }
        val app3 = GlobalScope.launch { startApplication(arrayOf("1", "2"), emptyMap()) }
        val app4 = GlobalScope.launch { startApplication(arrayOf("2", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        executeChange("$peer1/create_change")

        // then - transaction is executed in same peerset
        val peer2Change = httpClient.get<String>("$peer2/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer2Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - transaction is executed in other peerset
        val peer3Change = httpClient.get<String>("$peer3/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer2Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - there's only one change in history of both peersets
        httpClient.get<String>("$peer2/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }
            .let {
                expect {
                    that(it.size).isEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        httpClient.get<String>("$peer3/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }
            .let {
                expect {
                    that(it.size).isEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        listOf(app1, app2, app3, app4).forEach { app -> app.cancel() }
    }

    private val peer1 = "http://localhost:8081"
    private val peer2 = "http://localhost:8082"
    private val peer3 = "http://localhost:8083"
    private val peer4 = "http://localhost:8084"

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