package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectThat
import strikt.assertions.*
import java.util.concurrent.Phaser


@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class MixedChangesSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(MultiplePeersetSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }


    @Test
    fun `try to execute two following changes in the same time, first GPAC, then Raft`(): Unit = runBlocking {
        val change = change(0, 1)
        val secondChange = change(mapOf(0 to change.toHistoryEntry(PeersetId("peerset0")).getId()))

        val applyEndPhaser = Phaser(6)
        val beforeSendingApplyPhaser = Phaser(1)
        val electionPhaser = Phaser(4)
        val applyConsensusPhaser = Phaser(2)
        val receivedAgreePhaser = Phaser(5)

        listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, applyConsensusPhaser, receivedAgreePhaser)
            .forEach { it.register() }
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyEnd to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                applyEndPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected,
            Signal.BeforeSendingApply to SignalListener {
                runBlocking {
                    receivedAgreePhaser.arriveAndAwaitAdvanceWithTimeout()
                    beforeSendingApplyPhaser.arrive()
                }
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (it.change?.id == secondChange.id) applyConsensusPhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
        )

        val peer0Address = apps.getPeer("peer0").address

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://$peer0Address/v2/change/async?peerset=peerset0", change)

        beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://$peer0Address/v2/change/async?peerset=peerset0", secondChange)

        applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

        applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()


//      First peerset
        askAllForChanges("peerset0").forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(2)
            expectThat(changes[0]).isEqualTo(change)
            expectThat(changes[1]).isEqualTo(secondChange)
        }

        askAllForChanges("peerset1").forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    @Test
    fun `try to execute two following changes in the same time (two different peers), first GPAC, then Raft`(): Unit =
        runBlocking {
            val change = change(0, 1)
            val secondChange = change(mapOf(1 to change.toHistoryEntry(PeersetId("peerset0")).getId()))

            val applyEndPhaser = Phaser(6)
            val beforeSendingApplyPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applyConsensusPhaser = Phaser(3)
            val receivedAgreePhaser = Phaser(5)

            listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, receivedAgreePhaser)
                .forEach { it.register() }
            val leaderElected = SignalListener {
                logger.info("Arrived ${it.subject.getPeerName()}")
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.OnHandlingApplyEnd to SignalListener {
                    logger.info("Arrived: ${it.subject.getPeerName()}")
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.BeforeSendingApply to SignalListener {
                    runBlocking {
                        receivedAgreePhaser.arriveAndAwaitAdvanceWithTimeout()
                        beforeSendingApplyPhaser.arrive()
                    }

                },
                Signal.OnHandlingAgreeEnd to SignalListener { receivedAgreePhaser.arrive() },
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    logger.info("Arrived consensus change: ${it.subject.getPeerName()}")
                    if (it.change?.id == secondChange.id) applyConsensusPhaser.arrive()
                }
            )

            apps = TestApplicationSet(
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5"),
                ),
                signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
            )

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0", change)

            beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer("peer3").address}/v2/change/async?peerset=peerset1", secondChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges("peerset0").forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change)
            }

            askAllForChanges("peerset1").forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(2)
                expectThat(changes[0]).isEqualTo(change)
                expectThat(changes[1]).isEqualTo(secondChange)
            }
        }

    @Test
    fun `try to execute two following changes in the same time, first 2PC, then Raft`(): Unit = runBlocking {
        val firstChange = change(0, 1)
        val secondChange = change(mapOf(0 to firstChange.toHistoryEntry(PeersetId("peerset0")).getId()))
        val thirdChange = change(mapOf(1 to firstChange.toHistoryEntry(PeersetId("peerset1")).getId()))


        val applyEndPhaser = Phaser(1)
        val beforeSendingApplyPhaser = Phaser(1)
        val electionPhaser = Phaser(4)
        val applySecondChangePhaser = Phaser(2)
        val applyThirdChangePhaser = Phaser(2)

        listOf(
            applyEndPhaser,
            electionPhaser,
            beforeSendingApplyPhaser,
            applySecondChangePhaser,
            applyThirdChangePhaser,
        )
            .forEach { it.register() }
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.TwoPCOnChangeApplied to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                applyEndPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected,
            Signal.TwoPCOnChangeAccepted to SignalListener {
                beforeSendingApplyPhaser.arrive()
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (it.change?.id == secondChange.id) applySecondChangePhaser.arrive()
                if (it.change?.id == thirdChange.id) applyThirdChangePhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true", firstChange)

        beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0", secondChange)
        executeChange("http://${apps.getPeer("peer4").address}/v2/change/async?peerset=peerset1", thirdChange)

        applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

        applySecondChangePhaser.arriveAndAwaitAdvanceWithTimeout()
        applyThirdChangePhaser.arriveAndAwaitAdvanceWithTimeout()


//      First peerset
        askAllForChanges("peerset0").forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(3)
            expectThat(changes[1].id).isEqualTo(firstChange.id)
            expectThat(changes[2].id).isEqualTo(secondChange.id)
        }

        askAllForChanges("peerset1").forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(3)
            expectThat(changes[1].id).isEqualTo(firstChange.id)
            expectThat(changes[2].id).isEqualTo(thirdChange.id)
        }
    }

    @Test
    fun `try to execute two following changes in the same time (two different peers), first 2PC, then Raft`(): Unit =
        runBlocking {
            val change = change(0, 1)
            val secondChange = change(mapOf(1 to change.toHistoryEntry(PeersetId("peerset0")).getId()))

            val beforeSendingApplyPhaser = Phaser(1)
            val applyEndPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applyConsensusPhaser = Phaser(2)
            val apply2PCPhaser = Phaser(4)
            listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, applyConsensusPhaser, apply2PCPhaser)
                .forEach { it.register() }

            val leaderElected = SignalListener {
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.TwoPCOnChangeApplied to SignalListener {
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.TwoPCOnChangeAccepted to SignalListener {
                    beforeSendingApplyPhaser.arrive()
                },
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    if (it.change?.id == change.id) {
                        apply2PCPhaser.arrive()
                    }
                    if (it.change?.id == secondChange.id) {
                        applyConsensusPhaser.arrive()
                    }
                }
            )

            apps = TestApplicationSet(
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5"),
                ),
                signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
            )

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true", change)

            beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer("peer3").address}/v2/change/async?peerset=peerset1", secondChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()

            apply2PCPhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges("peerset0").forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(2)
                expectThat(changes[1].id).isEqualTo(change.id)
            }

            askAllForChanges("peerset1").forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(3)
                expectThat(changes[1].id).isEqualTo(change.id)
                expectThat(changes[2].id).isEqualTo(secondChange.id)
            }
        }

    @Test
    fun `try to execute two following changes, first 2PC, then Raft`(): Unit =
        runBlocking {
            val firstChange = change(0, 1)
            val secondChange = change(mapOf(1 to firstChange.toHistoryEntry(PeersetId("peerset1")).getId()))
            val thirdChange = change(mapOf(0 to firstChange.toHistoryEntry(PeersetId("peerset0")).getId()))

            val applyEndPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applySecondChangePhaser = Phaser(2)
            val applyThirdChangePhaser = Phaser(2)

            listOf(applyEndPhaser, applyThirdChangePhaser, applySecondChangePhaser, electionPhaser)
                .forEach { it.register() }
            val leaderElected = SignalListener {
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.TwoPCOnChangeApplied to SignalListener {
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    if (it.change?.id == thirdChange.id) {
                        applyThirdChangePhaser.arrive()
                    }
                    if (it.change?.id == secondChange.id) {
                        applySecondChangePhaser.arrive()
                    }
                }
            )

            apps = TestApplicationSet(
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5"),
                ),
                signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
            )

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true", firstChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer("peer3").address}/v2/change/async?peerset=peerset1", secondChange)
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0", thirdChange)
            applySecondChangePhaser.arriveAndAwaitAdvanceWithTimeout()
            applyThirdChangePhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges("peerset0").forEach {
                val changes = it.second
                expectThat(changes.size).isEqualTo(3)
                expectThat(changes[1].id).isEqualTo(firstChange.id)
                expectThat(changes[2].id).isEqualTo(thirdChange.id)
            }

            askAllForChanges("peerset1").forEach {
                val changes = it.second
                expectThat(changes.size).isEqualTo(3)
                expectThat(changes[1].id).isEqualTo(firstChange.id)
                expectThat(changes[2].id).isEqualTo(secondChange.id)
            }
        }

    private suspend fun executeChange(uri: String, change: Change): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peerAddress: PeerAddress, peersetId: String) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/changes?peerset=$peersetId") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peersetId: String) =
        apps.getPeerAddresses(peersetId).values.map { Pair(it, askForChanges(it, peersetId)) }

    private fun change(vararg peersetIds: Int) = AddUserChange(
        "userName",
        peersets = peersetIds.map {
            ChangePeersetInfo(PeersetId("peerset$it"), InitialHistoryEntry.getId())
        },
    )

    private fun change(peerSetIdToId: Map<Int, String>) = AddUserChange(
        "userName",
        peersets = peerSetIdToId.map {
            ChangePeersetInfo(PeersetId("peerset${it.key}"), it.value)
        },
    )
}
