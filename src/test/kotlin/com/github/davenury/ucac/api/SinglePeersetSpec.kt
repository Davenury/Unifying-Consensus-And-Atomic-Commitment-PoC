package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Agree
import com.github.davenury.ucac.commitment.gpac.Agreed
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.utils.IntegrationTestBase
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
class SinglePeersetSpec : IntegrationTestBase() {

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
                executeChange("http://${it.peers[0][1].address}/v2/change/sync?enforce_gpac=true", change(0))
            }.isSuccess()
            signalExecuted.set(true)
            throw RuntimeException("Stop")
        }

        apps = TestApplicationSet(
            listOf(3),
            signalListeners = mapOf(
                0 to mapOf(Signal.BeforeSendingAgree to signalListener)
            ),
        )

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?enforce_gpac=true", change(0))
        }
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {
            val changeAbortedPhaser = Phaser(1)
            changeAbortedPhaser.register()

            val change = change(0)

            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to SignalListener {
                    expectCatching {
                        executeChange(
                            "http://${it.peers[0][1].address}/v2/change/sync?enforce_gpac=true",
                            change,
                        )
                    }
                },
            )

            val signalListenersForCohort = mapOf(
                Signal.ReachedMaxRetries to SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    changeAbortedPhaser.arrive()
                },
            )

            apps = TestApplicationSet(
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

            expectCatching {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?enforce_gpac=true", change)
            }.isSuccess()

            changeAbortedPhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            try {
                testHttpClient.get<HttpResponse>(
                    "http://${apps.getPeer(0, 2).address}/v2/change_status/${change.id}"
                ) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expect {
                    that(e).isA<ServerResponseException>()
                    that(e.message).isNotNull()
                        .contains("Transaction failed due to too many retries of becoming a leader.")
                }
            }
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {
            val phaser = Phaser(3)
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
            val afterHandlingApply = SignalListener {
                phaser.arrive()
            }

            val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.BeforeSendingAgree to firstLeaderAction,
                Signal.OnHandlingApplyCommitted to afterHandlingApply
            )
            val otherPeersCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.OnHandlingApplyCommitted to afterHandlingApply,
            )

            apps = TestApplicationSet(
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

            // change that will cause leader to fall according to action
            val change = change(0)
            try {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?enforce_gpac=true", change)
                fail("Change passed")
            } catch (e: Exception) {
                logger.info("Leader 1 fails", e)
            }

            phaser.arriveAndAwaitAdvanceWithTimeout()

            val response = testHttpClient.get<Change>("http://${apps.getPeer(0, 1).address}/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            expectThat(response).isEqualTo(change)
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
        val phaserPeer1 = Phaser(2)
        val phaserAllPeers = Phaser(5)

        val proposedChange = AddGroupChange(
            "name",
            peersets = listOf(
                ChangePeersetInfo(0, InitialHistoryEntry.getId())
            ),
        )

        val firstLeaderAction = SignalListener { signalData ->
            val url = "http://${signalData.peers[0][0].address}/apply"
            runBlocking {
                testHttpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        signalData.transaction!!.ballotNumber,
                        true,
                        Accept.COMMIT,
                        signalData.change!!
                    )
                }.also {
                    logger.info("Got response ${it.status.value}")
                }
            }
            throw RuntimeException("Stop leader after apply")
        }

        val peer1Action = SignalListener { phaserPeer1.arrive() }
        val peersAction = SignalListener { phaserAllPeers.arrive() }

        val firstPeerSignals = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction,
            Signal.OnHandlingApplyCommitted to peersAction,
        )

        val peerSignals =
            mapOf(Signal.OnHandlingApplyCommitted to peersAction)

        val peer1Signals =
            mapOf(Signal.OnHandlingApplyCommitted to peer1Action)

        apps = TestApplicationSet(
            listOf(5),
            signalListeners = mapOf(
                0 to firstPeerSignals,
                1 to peer1Signals,
                2 to peerSignals,
                3 to peerSignals,
                4 to peerSignals,
            ),
            configOverrides = mapOf(
                0 to mapOf("raft.isEnabled" to false),
                1 to mapOf("raft.isEnabled" to false),
                2 to mapOf("raft.isEnabled" to false),
                3 to mapOf("raft.isEnabled" to false),
                4 to mapOf("raft.isEnabled" to false),
            )
        )

        // change that will cause leader to fall according to action
        try {
            executeChange(
                "http://${apps.getPeer(0, 0).address}/v2/change/sync?enforce_gpac=true",
                proposedChange
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserPeer1.arriveAndAwaitAdvanceWithTimeout()

        val change = testHttpClient.get<Change>("http://${apps.getPeer(0, 1).address}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(proposedChange.groupName)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserAllPeers.arriveAndAwaitAdvanceWithTimeout()

        apps.getPeers(0).forEach { (_, peerAddress) ->
            // and should not execute this change couple of times
            val changes = testHttpClient.get<Changes>("http://${peerAddress.address}/changes") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            // only one change and this change shouldn't be applied two times
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expect {
                that(changes[0]).isA<AddGroupChange>()
                that((changes[0] as AddGroupChange).groupName).isEqualTo(proposedChange.groupName)
            }
        }
    }

    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private fun change(vararg peersets: Int) = AddUserChange(
        "userName",
        peersets = peersets.map {
            ChangePeersetInfo(it, InitialHistoryEntry.getId())
        },
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }
}
