package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Agree
import com.github.davenury.ucac.commitment.gpac.Agreed
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean


@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class SinglePeersetSpec {

    companion object {
        private val logger = LoggerFactory.getLogger(SinglePeersetSpec::class.java)!!
    }

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
                executeChange("http://${it.peers[0][1].address}/v2/change/sync?enforce_gpac=true", change(listOf()))
            }.isSuccess()
            signalExecuted.set(true)
            throw RuntimeException("Stop")
        }

        val apps = TestApplicationSet(
            listOf(3),
            signalListeners = mapOf(
                0 to mapOf(Signal.BeforeSendingAgree to signalListener)
            ),
        )
        val peers = apps.getPeers()

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/v2/change/sync?enforce_gpac=true", change(listOf()))
        }

        apps.stopApps()
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {
            val changeAbortedPhaser = Phaser(1)
            changeAbortedPhaser.register()

            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to SignalListener {
                    expectCatching {
                        executeChange("http://${it.peers[0][1].address}/v2/change/sync?enforce_gpac=true", change(listOf()))
                    }
                },
            )

            val signalListenersForCohort = mapOf(
                Signal.ReachedMaxRetries to SignalListener {
                    changeAbortedPhaser.arrive()
                },
            )

            val apps = TestApplicationSet(
                listOf(3),
                signalListeners = mapOf(
                    0 to signalListenersForLeader,
                    1 to signalListenersForCohort,
                    2 to signalListenersForCohort,
                ),
                configOverrides = mapOf(
                    1 to mapOf("gpac.retriesBackoffTimeout" to Duration.ZERO),
                    2 to mapOf("gpac.retriesBackoffTimeout" to Duration.ZERO),
                )
            )
            val peers = apps.getPeers()

            expectCatching {
                executeChange("http://${peers[0][0]}/v2/change/sync?enforce_gpac=true", change(listOf()))
            }.isSuccess()

            changeAbortedPhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            try {
                testHttpClient.get<HttpResponse>(
                    "http://${peers[0][2]}/v2/change_status/${
                        change(listOf(peers[0][2])).toHistoryEntry().getId()
                    }"
                ) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expect {
                    that(e).isA<ServerResponseException>()
                    that(e.message).isNotNull().contains("Transaction failed due to too many retries of becoming a leader.")
                }
            }

            apps.stopApps()
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {
            val phaser = Phaser(2)
            phaser.register()

            val firstLeaderAction = SignalListener {
                runBlocking {
                    testHttpClient.post<Agreed>("http://${it.peers[0][1].address}/ft-agree") {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Agree(it.transaction!!.ballotNumber, Accept.COMMIT, it.change!!)
                    }
                    throw RuntimeException("Stop")
                }
            }
            val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.BeforeSendingAgree to firstLeaderAction,
            )
            val afterHandlingApply = SignalListener {
                runBlocking {
                    phaser.arriveAndAwaitAdvanceWithTimeout()
                }
            }
            val otherPeersCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.OnHandlingApplyCommitted to afterHandlingApply,
            )

            val apps = TestApplicationSet(
                listOf(3),
                signalListeners = mapOf(
                    0 to firstLeaderCallbacks,
                    1 to otherPeersCallbacks,
                    2 to otherPeersCallbacks,
                ),
                configOverrides = mapOf(
                    1 to mapOf("gpac.leaderFailTimeout" to Duration.ZERO),
                ),
            )
            val peers = apps.getPeers()

            // change that will cause leader to fall according to action
            try {
                executeChange("http://${peers[0][0]}/v2/change/sync?enforce_gpac=true", change(listOf()))
                fail("Change passed")
            } catch (e: Exception) {
                logger.info("Leader 1 fails", e)
            }

            phaser.arriveAndAwaitAdvanceWithTimeout()

            val response = testHttpClient.get<Change>("http://${peers[0][1]}/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            expectThat(response).isEqualTo(change(listOf(peers[0][0])))

            apps.stopApps()
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
        val phaser = Phaser(2)

        val firstLeaderAction = SignalListener { signalData ->
            val url = "http://${signalData.peers[0][1].address}/apply"
            runBlocking {
                testHttpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        signalData.transaction!!.ballotNumber,
                        true,
                        Accept.COMMIT,
                        AddGroupChange(
                            InitialHistoryEntry.getId(),
                            "groupName",
                            listOf("http://${signalData.peers[0][1].address}"),
                        )
                    )
                }.also {
                    logger.info("Got response ${it.status.value}")
                }
            }
            throw RuntimeException()
        }
        val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction
        )

        val peer3Action = SignalListener {
            runBlocking {
                phaser.arriveAndAwaitAdvanceWithTimeout()
            }
        }
        val peer3Callbacks: Map<Signal, SignalListener> = mapOf(
            Signal.OnHandlingApplyEnd to peer3Action
        )

        val apps = TestApplicationSet(
            listOf(5),
            signalListeners = mapOf(
                0 to firstLeaderCallbacks,
                2 to peer3Callbacks,
            )
        )
        val peers = apps.getPeers()

        // change that will cause leader to fall according to action
        try {
            executeChange(
                "http://${peers[0][0]}/v2/change/sync?enforce_gpac=true", AddGroupChange(
                    InitialHistoryEntry.getId(),
                    "name",
                    listOf(),
                )
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaser.arriveAndAwaitAdvanceWithTimeout()

        val change = testHttpClient.get<Change>("http://${peers[0][2]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo("name")
        }

        // and should not execute this change couple of times
        val changes = testHttpClient.get<Changes>("http://${peers[0][1]}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        // only one change and this change shouldn't be applied two times
        expectThat(changes.size).isGreaterThanOrEqualTo(1)
        expect {
            that(changes[0]).isA<AddGroupChange>()
            that((changes[0] as AddGroupChange).groupName).isEqualTo("name")
        }

        apps.stopApps()
    }

    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private fun change(peers: List<String>) = AddUserChange(
        InitialHistoryEntry.getId(),
        "userName",
        // leader should enrich himself
        peers,
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }
}
