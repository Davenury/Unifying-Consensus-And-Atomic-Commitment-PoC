package com.github.davenury.ucac.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.AddGroupChange
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.ChangeDto
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNumDto
import com.github.davenury.ucac.consensus.ratis.HistoryDto
import com.github.davenury.ucac.gpac.domain.*
import com.github.davenury.ucac.utils.TestApplicationSet
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.util.*
import kotlin.random.Random
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean

class SinglePeersetIntegrationTest {

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "single_peerset_application.conf")
        deleteRaftHistories()
    }

    @Test
    fun `second leader tries to become leader before first leader goes into ft-agree phase`(): Unit = runBlocking {
        val signalExecuted = AtomicBoolean(false)

        val signalListener = SignalListener {
            expectCatching {
                executeChange("http://${it.otherPeers[0][0]}/create_change")
            }.isSuccess()
            signalExecuted.set(true)
            throw RuntimeException("Stop")
        }

        val apps = TestApplicationSet(
            1, listOf(3),
            signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingAgree to signalListener))
        )
        val peers = apps.getPeers()

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/create_change")
        }

        apps.stopApps()
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {

            val signalListener = SignalListener {
                expectCatching {
                    executeChange("http://${it.otherPeers[0][0]}/create_change")
                }.isFailure()
            }

            val apps = TestApplicationSet(1, listOf(3),
                signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingApply to signalListener)),
            )
            val peers = apps.getPeers()

            expectCatching {
                executeChange("http://${peers[0][0]}/create_change")
            }.isSuccess()

            apps.stopApps()
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {

            val phaser = Phaser(2)

            val firstLeaderAction = SignalListener { runBlocking {
                val url = "http://${it.otherPeers[0][0]}/ft-agree"
                val response = testHttpClient.post<Agreed>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(it.transaction!!.ballotNumber, Accept.COMMIT, changeDto)
                }
                throw RuntimeException("Stop")
            }}
            val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.BeforeSendingAgree to firstLeaderAction
            )
            val afterHandlingApply = SignalListener {
                phaser.arriveAndAwaitAdvance()
            }
            val peer2Callbacks: Map<Signal, SignalListener> = mapOf(
                Signal.OnHandlingApplyEnd to afterHandlingApply
            )

            val apps = TestApplicationSet(1, listOf(3),
                signalListeners = mapOf(
                    1 to firstLeaderCallbacks,
                    2 to peer2Callbacks
                ),
                configOverrides = mapOf(2 to mapOf("protocol.leaderFailTimeout" to Duration.ZERO))
            )
            val peers = apps.getPeers()

            // change that will cause leader to fall according to action
            try {
                executeChange("http://${peers[0][0]}/create_change")
                fail("Change passed")
            } catch (e: Exception) {
                println("Leader 1 fails: $e")
            }

            phaser.arriveAndAwaitAdvance()
            val response = testHttpClient.get<String>("http://${peers[0][2]}/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            val change = objectMapper.readValue<ChangeWithAcceptNumDto>(response).let {
                ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
            }
            expectThat(change.change).isEqualTo(AddUserChange("userName"))

            apps.stopApps()
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
        val phaser = Phaser(2)

        val configOverrides = mapOf<String, Any>(
            "peers.peersAddresses" to listOf(createPeersInRange(5)),
        )

        val firstLeaderAction = SignalListener {
            val url = "http://${it.otherPeers[0][0]}/apply"
            runBlocking {
                testHttpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        it.transaction!!.ballotNumber,
                        true,
                        Accept.COMMIT,
                        ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
                    )
                }.also {
                    println("Got response ${it.status.value}")
                }
            }
            throw RuntimeException()
        }
        val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction
        )

        val peer3Action = SignalListener {
            phaser.arriveAndAwaitAdvance()
        }
        val peer3Callbacks: Map<Signal, SignalListener> = mapOf(
            Signal.OnHandlingApplyEnd to peer3Action
        )

        val apps = TestApplicationSet(1, listOf(5),
            signalListeners = mapOf(
                1 to firstLeaderCallbacks,
                3 to peer3Callbacks
            ),
            configOverrides = (1..5).zip(List(5) { configOverrides }).toMap()
        )
        val peers = apps.getPeers()

        // change that will cause leader to fall according to action
        try {
            executeChange("http://${peers[0][0]}/create_change", mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
            fail("Change passed")
        } catch (e: Exception) {
            println("Leader 1 fails: $e")
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaser.arriveAndAwaitAdvance()

        val response = testHttpClient.get<String>("http://${peers[0][3]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val change = objectMapper.readValue(response, ChangeWithAcceptNumDto::class.java)
        expectThat(change.change.toChange()).isEqualTo(AddGroupChange("name"))

        // and should not execute this change couple of times
        val response2 = testHttpClient.get<String>("http://${peers[0][1]}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val values: List<ChangeWithAcceptNum> = objectMapper.readValue<HistoryDto>(response2).changes.map {
            ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
        }
        // only one change and this change shouldn't be applied for 8082 two times
        expect {
            that(values.size).isGreaterThanOrEqualTo(1)
            that(values[0]).isEqualTo(ChangeWithAcceptNum(AddGroupChange("name"), 1))
        }

        apps.stopApps()
    }

    private fun createPeersInRange(range: Int): List<String> =
        List(range) { "localhost:${8081 + it}" }

    private suspend fun executeChange(uri: String, change: Map<String, Any> = changeDto.properties) =
        testHttpClient.post<String>(uri) {
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
