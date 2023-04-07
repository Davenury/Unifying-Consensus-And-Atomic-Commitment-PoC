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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean

@Disabled("Temporary")
@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class SinglePeersetSpec : IntegrationTestBase() {

    companion object {
        private val logger = LoggerFactory.getLogger(SinglePeersetSpec::class.java)!!
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "single_peerset_application.conf")
    }

    @Test
    fun `second leader tries to become leader before first leader goes into ft-agree phase`(): Unit = runBlocking {
        val signalExecuted = AtomicBoolean(false)

        val signalListener = SignalListener {
            expectCatching {
                val peer1Address: String = it.peerResolver.resolve("peer1").address
                executeChange("http://$peer1Address/v2/change/sync?enforce_gpac=true", change())
            }.isSuccess()
            signalExecuted.set(true)
            throw RuntimeException("Stop")
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2")
            ),
            signalListeners = mapOf(
                "peer0" to mapOf(Signal.BeforeSendingAgree to signalListener)
            ),
        )

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?enforce_gpac=true", change())
        }
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {
            val waitForAbortChangePhaser = Phaser(2)

            val change1 = change()
            val change2 = change()

            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to SignalListener {
                    expectCatching {
                        val peer1Address = it.peerResolver.resolve("peer2").address
                        logger.info("Sending change to $peer1Address")
                        executeChange(
                            "http://$peer1Address/v2/change/sync?enforce_gpac=true",
                            change2,
                        )
                    }.isFailure()
                    waitForAbortChangePhaser.arrive()
                },
            )

            val signalListenersForCohort = mapOf(
                Signal.ReachedMaxRetries to SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    waitForAbortChangePhaser.arrive()
                },
            )

            val config = mapOf(
                "gpac.retriesBackoffTimeout" to Duration.ZERO,
                "gpac.initialRetriesDelay" to Duration.ZERO,
                "gpac.leaderFailDelay" to Duration.ofSeconds(1),
                "gpac.leaderFailBackoff" to Duration.ZERO,
            )

            apps = TestApplicationSet(
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2")
                ),
                signalListeners = mapOf(
                    "peer0" to signalListenersForLeader,
                    "peer2" to signalListenersForCohort,
                ),
                configOverrides = mapOf(
                    "peer0" to config,
                    "peer2" to config,
                )
            )

            expectCatching {
                executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?enforce_gpac=true", change1)
            }.isSuccess()

            try {
                val peer2Address = apps.getPeer("peer2").address
                testHttpClient.get<HttpResponse>(
                    "http://$peer2Address/v2/change_status/${change2.id}"
                ) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expect {
                    logger.error("Error", e)
                    that(e).isA<ServerResponseException>()
                    that(e.message).isNotNull()
                        .contains("Transaction failed due to too many retries of becoming a leader.")
                }
            }
        }

    @Disabled("Temporary")
    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {
            val phaser = Phaser(3)
            phaser.register()

            val firstLeaderAction = SignalListener {
                runBlocking {
                    val peer1Address = it.peerResolver.resolve("peer1").address
                    testHttpClient.post<Agreed>("http://$peer1Address/ft-agree") {
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
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2")
                ),
                signalListeners = mapOf(
                    "peer0" to firstLeaderCallbacks,
                    "peer1" to otherPeersCallbacks,
                    "peer2" to otherPeersCallbacks,
                ),
                configOverrides = mapOf(
                    "peer1" to mapOf("gpac.leaderFailTimeout" to Duration.ZERO),
                ),
            )

            // change that will cause leader to fall according to action
            val change = change()
            try {
                val peer0Address = apps.getPeer("peer0").address
                executeChange("http://$peer0Address/v2/change/sync?enforce_gpac=true", change)
                fail("Change passed")
            } catch (e: Exception) {
                logger.info("Leader 1 fails", e)
            }

            phaser.arriveAndAwaitAdvanceWithTimeout()

            val peer1Address = apps.getPeer("peer1").address
            val response = testHttpClient.get<Change>("http://$peer1Address/change") {
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
                ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId())
            ),
        )

        val firstLeaderAction = SignalListener { signalData ->
            val url = "http://${signalData.peerResolver.resolve("peer1").address}/apply"
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4")
            ),
            signalListeners = mapOf(
                "peer0" to firstPeerSignals,
                "peer1" to peer1Signals,
                "peer2" to peerSignals,
                "peer3" to peerSignals,
                "peer4" to peerSignals,
            ),
            configOverrides = mapOf(
                "peer0" to mapOf("consensus.isEnabled" to false),
                "peer1" to mapOf("consensus.isEnabled" to false, "gpac.leaderFailDelay" to java.time.Duration.ZERO),
                "peer2" to mapOf("consensus.isEnabled" to false),
                "peer3" to mapOf("consensus.isEnabled" to false),
                "peer4" to mapOf("consensus.isEnabled" to false),
            )
        )

        // change that will cause leader to fall according to action
        try {
            val peer0Address = apps.getPeer("peer0").address
            executeChange(
                "http://$peer0Address/v2/change/sync?enforce_gpac=true",
                proposedChange
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserPeer1.arriveAndAwaitAdvanceWithTimeout()

        val peer1Address = apps.getPeer("peer1").address
        val change = testHttpClient.get<Change>("http://$peer1Address/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(proposedChange.groupName)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserAllPeers.arriveAndAwaitAdvanceWithTimeout()

        apps.getPeerAddresses("peerset0").forEach { (_, peerAddress) ->
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

    private fun change() = AddUserChange(
        "userName",
        peersets = listOf(
            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
        ),
    )
}
