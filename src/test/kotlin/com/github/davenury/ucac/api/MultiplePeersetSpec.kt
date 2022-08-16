package com.github.davenury.ucac.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.ChangeDto
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNumDto
import com.github.davenury.ucac.consensus.ratis.HistoryDto
import com.github.davenury.ucac.gpac.domain.Accept
import com.github.davenury.ucac.gpac.domain.Apply
import com.github.davenury.ucac.utils.TestApplicationSet
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.contains
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo
import java.io.File
import java.util.*
import kotlin.random.Random

class MultiplePeersetSpec {

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {
        val apps = TestApplicationSet(2, listOf(3, 3))
        val peers = apps.getPeers()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/create_change")

        // then - transaction is executed in same peerset
        val peer2Change = httpClient.get<String>("http://${peers[0][1]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer2Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - transaction is executed in other peerset
        val peer4Change = httpClient.get<String>("http://${peers[1][0]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer4Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - there's only one change in history of both peersets
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges("http://${peers[1][0]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        apps.stopApps()
    }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit = runBlocking {

        val apps = TestApplicationSet(2, listOf(3, 3), appsToExclude = listOf(4, 5, 6))
        val peers = apps.getPeers()

        // when - executing transaction
        try {
            executeChange("http://${peers[0][0]}/create_change")
            fail("Exception not thrown")
        } catch (e: Exception) {
            expect {
                that(e).isA<ServerResponseException>()
                that(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
            }
        }

        // then - transaction should not be executed
        askForChanges("http://${peers[0][2]}")
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        apps.stopApps()
    }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {

        val appsToExclude = listOf(3, 6, 7, 8)
        val apps = TestApplicationSet(2, listOf(3, 5), appsToExclude = appsToExclude)
        val peers = apps.getPeers()

        delay(5000)

        // when - executing transaction
        try {
            executeChange("http://${peers[0][0]}/create_change")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        apps.stopApps()
    }

    @Test
    fun `transaction should pass when more than half peers of all peersets are operative`(): Unit = runBlocking {
        val appsToExclude = listOf(3, 7, 8)
        val apps = TestApplicationSet(2, listOf(3, 5), appsToExclude = appsToExclude)
        val peers = apps.getPeers()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/create_change")

        // then - transaction should be executed in every peerset
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges("http://${peers[1][0]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        apps.stopApps()
    }

    @Test
    fun `transaction should not be processed if every peer from one peerset fails after ft-agree`(): Unit =
        runBlocking {

            val failAction = SignalListener {
                throw RuntimeException()
            }
            val apps = TestApplicationSet(
                2, listOf(3, 5),
                signalListeners = (4..8).zip(List(5) { mapOf(Signal.OnHandlingAgreeEnd to failAction) }).toMap()
            )
            val peers = apps.getPeers()

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("http://${peers[0][0]}/create_change")
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too few responses of ft phase.")
            }

            peers.flatten().forEach {
                askForChanges("http://${it}")
                    .let {
                        expectThat(it.size).isEqualTo(0)
                    }
            }

            apps.stopApps()
        }

    @Disabled("Delays are still used")
    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit = runBlocking {

        val failAction = SignalListener {
            throw RuntimeException()
        }

        val apps = TestApplicationSet(
            2, listOf(3, 5),
            signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingApply to failAction))
        )
        val peers = apps.getPeers()

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/create_change")
        }

        // application should elect recovery leader to perform transaction to the end
        delay(20000)

        peers.flatten().forEach {
            askForChanges("http://$it")
                .let {
                    expect {
                        that(it.size).isGreaterThanOrEqualTo(1)
                        that(it[0].change).isEqualTo(AddUserChange("userName"))
                    }
                }
        }

        apps.stopApps()
    }

    @Disabled("Delays are still used")
    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {

            val leaderAction = SignalListener {
                val url2 = "${it.otherPeers[0][0]}/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT, ChangeDto(
                                mapOf(
                                    "operation" to "ADD_USER",
                                    "userName" to "userName"
                                )
                            )
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("${it.otherPeers[0][0]} sent response to apply")
                val url3 = "${it.otherPeers[0][1]}/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url3) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT, ChangeDto(
                                mapOf(
                                    "operation" to "ADD_USER",
                                    "userName" to "userName"
                                )
                            )
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("${it.otherPeers[0][1]} sent response to apply")
                throw RuntimeException()
            }

            val apps = TestApplicationSet(
                2, listOf(3, 5),
                signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingApply to leaderAction))
            )
            val peers = apps.getPeers()

            delay(5000)

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChange("http://${peers[0][0]}/create_change")
            }

            // application should elect recovery leader to perform transaction to the end
            delay(20000)

            peers.flatten().forEach {
                askForChanges("http://$it")
                    .let {
                        expect {
                            that(it.size).isGreaterThanOrEqualTo(1)
                            that(it[0].change).isEqualTo(AddUserChange("userName"))
                        }
                    }
            }

            apps.stopApps()
        }


    private suspend fun executeChange(uri: String, change: Map<String, Any> = changeDto.properties) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peer: String) =
        testHttpClient.get<String>("$peer/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

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
