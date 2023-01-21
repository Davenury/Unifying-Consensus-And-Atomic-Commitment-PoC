package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestApplicationSet.Companion.NON_RUNNING_PEER
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
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
class TwoPCSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(TwoPCSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(8)
        val electionPhaser = Phaser(4)
        listOf(changeAppliedPhaser, electionPhaser).forEach { it.register() }

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                changeAppliedPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() }
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeers().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                .with(TwoPCChange::change) { isEqualTo(change) }
            expectThat(changes[1]).isA<AddUserChange>()
                .with(AddUserChange::userName) { isEqualTo("userName") }
        }
    }

    @Test
    fun `should be able to execute 2 transactions`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(4)
        val changeSecondAppliedPhaser = Phaser(4)
        val electionPhaser = Phaser(4)
        listOf(changeAppliedPhaser, changeSecondAppliedPhaser, electionPhaser).forEach { it.register() }

        val change = change(0, 1)

        val historyEntryId = { peersetId: Int ->
            change.toHistoryEntry(peersetId).getId()
        }

        val changeSecond = change(Pair(0, historyEntryId(0)), Pair(1, historyEntryId(1)))

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if(it.change!!.id == change.id) changeAppliedPhaser.arrive()
                if(it.change!!.id == changeSecond.id) changeSecondAppliedPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() }
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", changeSecond)

        changeSecondAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeers().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(4)
            expectThat(changes[0]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                .with(TwoPCChange::change) { isEqualTo(change) }
            expectThat(changes[1]).isA<AddUserChange>()
                .with(AddUserChange::userName) { isEqualTo("userName") }
            expectThat(changes[2]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                .with(TwoPCChange::change) { isEqualTo(changeSecond) }
            expectThat(changes[3]).isA<AddUserChange>()
                .with(AddUserChange::userName) { isEqualTo("userName") }
        }
    }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(4)
        val appliedChangesListener = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            changeAppliedPhaser.arrive()
        }

        val electionPhaser = Phaser(2)
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        listOf(electionPhaser, changeAppliedPhaser).forEach { it.register() }

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to appliedChangesListener,
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            appsToExclude = listOf(3, 4, 5),
            signalListeners = (0..5).associateWith { signalListenersForCohort },
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val change: Change = change(0, 1)
        val result = executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", change)

        expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should not be executed
        askAllForChanges(apps.getPeers(0).values).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) {
                    isEqualTo(TwoPCStatus.ACCEPTED)
                }
                .with(TwoPCChange::change) {
                    isEqualTo(change)
                }
            expectThat(changes[1]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) {
                    isEqualTo(TwoPCStatus.ABORTED)
                }
                .with(TwoPCChange::change) {
                    isEqualTo(change)
                }
        }

        try {
            testHttpClient.get<HttpResponse>(
                "http://${apps.getPeer(0, 0).address}/v2/change_status/${change.id}"
            ) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }
            fail("executing change didn't fail")
        } catch (e: ClientRequestException) {
            expectThat(e.message).contains("Change conflicted")
        }
    }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {
        apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 5, 6, 7),
        )
        val change = change(0, 1)

        delay(5000)

        // when - executing transaction
        try {
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?use_2pc=true", change)
            fail("Exception not thrown")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askAllForChanges(apps.getPeers(0).values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }
    }

    @Test
    fun `transaction should pass when more than half peers of all peersets are operative`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(6)
        changeAppliedPhaser.register()

        val peerApplyCommitted = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            changeAppliedPhaser.arrive()
        }

        val electionPhaser = Phaser(3)
        electionPhaser.register()
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to peerApplyCommitted,
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 6, 7),
            signalListeners = (0..7).associateWith { signalListenersForCohort },
        )
        val peers = apps.getPeers()
        val change: Change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should be executed in every peerset
        askAllForChanges(peers.values.filter { it.address != NON_RUNNING_PEER }).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isA<TwoPCChange>()
                .with(Change::peersets) { isEqualTo(change.peersets) }
                .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                .with(TwoPCChange::change) { isEqualTo(change) }
            expectThat(changes[1]).isA<AddUserChange>()
                .with(AddUserChange::userName) { isEqualTo("userName") }
        }
    }

    @Test
    fun `transaction should be processed if peer from second peerset accepted change`(): Unit =
        runBlocking {
            val firstPeersetChangeAppliedPhaser = Phaser(4)
            val secondPeersetChangeAppliedPhaser = Phaser(8)
            val electionPhaser = Phaser(6)
            val leaderElected = SignalListener {
                electionPhaser.arrive()
            }
            listOf(
                firstPeersetChangeAppliedPhaser,
                secondPeersetChangeAppliedPhaser,
                electionPhaser
            ).forEach { it.register() }

            val isChangeNotAccepted = AtomicBoolean(true)

            val onHandleDecision = SignalListener {
                if (isChangeNotAccepted.get()) throw Exception("Simulate ignoring 2PC-decision message")
            }

            apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners =
                (0..2).associateWith {
                    mapOf(
                        Signal.ConsensusLeaderElected to leaderElected,
                        Signal.ConsensusFollowerChangeAccepted to
                                SignalListener { firstPeersetChangeAppliedPhaser.arrive() }
                    )
                }.toMap() +
                        (3..7).associateWith {
                            mapOf(
                                Signal.TwoPCOnHandleDecision to onHandleDecision,
                                Signal.ConsensusLeaderElected to leaderElected,
                                Signal.ConsensusFollowerChangeAccepted to
                                        SignalListener { secondPeersetChangeAppliedPhaser.arrive() }
                            )
                        }.toMap(),
            )
            val peers = apps.getPeers()
            val change: Change = change(0, 1)

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?use_2pc=true", change)

            firstPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            isChangeNotAccepted.set(false)

            secondPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges(peers.values).forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(TwoPCChange::peersets) {
                        isEqualTo(change.peersets)
                    }
                    .with(TwoPCChange::twoPCStatus) {
                        isEqualTo(TwoPCStatus.ACCEPTED)
                    }
                    .with(TwoPCChange::change) {
                        isEqualTo(change)
                    }
                expectThat(changes[1]).isA<AddUserChange>()
                    .with(AddUserChange::userName) { isEqualTo("userName") }
            }
        }

    @Disabled("I am not sure if this is solid case in 2PC")
    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit = runBlocking {
        val failAction = SignalListener {
            throw RuntimeException("Leader failed after ft-agree")
        }

        val applyCommittedPhaser = Phaser(7)
        applyCommittedPhaser.register()

        val peerApplyCommitted = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            applyCommittedPhaser.arrive()
        }

        val signalListenersForLeaders = mapOf(
            Signal.BeforeSendingApply to failAction,
        )
        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyCommitted to peerApplyCommitted,
        )

        apps = TestApplicationSet(
            listOf(3, 5),
            signalListeners = mapOf(
                0 to signalListenersForLeaders,
                1 to signalListenersForCohort,
                2 to signalListenersForCohort,
                3 to signalListenersForCohort,
                4 to signalListenersForCohort,
                5 to signalListenersForCohort,
                6 to signalListenersForCohort,
                7 to signalListenersForCohort,
            )
        )
        val peers = apps.getPeers()
        val change = change(0, 1)

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?use_2pc=true", change)
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.values).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    @Test
    fun `should be able to execute change in two different peersets even if changes in peersets are different`() =
        runBlocking {
            val consensusLeaderElectedPhaser = Phaser(6)
            val firstChangePhaser = Phaser(2)
            val secondChangePhaser = Phaser(4)
            val finalChangePhaser = Phaser(6)

            listOf(firstChangePhaser, secondChangePhaser, finalChangePhaser, consensusLeaderElectedPhaser)
                .forEach { it.register() }

            val firstChangeListener = SignalListener {
                if (it.change!! is AddUserChange) {
                    firstChangePhaser.arrive()
                } else if (it.change is AddRelationChange) {
                    finalChangePhaser.arrive()
                }
            }

            val secondChangeListener = SignalListener {
                if (it.change!! is AddGroupChange) {
                    secondChangePhaser.arrive()
                } else if (it.change is AddRelationChange) {
                    finalChangePhaser.arrive()
                }
            }

            val leaderElectedListener = SignalListener {
                consensusLeaderElectedPhaser.arrive()
            }

            apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = List(3) {
                    it to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to firstChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap()
                        + List(5) {
                    it + 3 to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to secondChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap()
            )

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // given - change in first peerset
            val firstChange = AddUserChange(
                "firstUserName",
                peersets = listOf(ChangePeersetInfo(0, InitialHistoryEntry.getId())),
            )
            expectCatching {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", firstChange)
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // and - change in second peerset
            val secondChange = AddGroupChange(
                "firstGroup",
                peersets = listOf(ChangePeersetInfo(1, InitialHistoryEntry.getId())),
            )
            expectCatching {
                executeChange(
                    "http://${apps.getPeer(1, 0).address}/v2/change/sync",
                    secondChange
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing change between two peersets
            val lastChange: Change = AddRelationChange(
                "firstUserName",
                "firstGroup",
                peersets = listOf(
                    ChangePeersetInfo(0, firstChange.toHistoryEntry(0).getId()),
                    ChangePeersetInfo(1, secondChange.toHistoryEntry(1).getId()),
                ),
            )

            expectCatching {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync?use_2pc=true", lastChange)
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

//          First peerset
            askAllForChanges(apps.getPeers(0).values).forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(firstChange)
                expectThat(it[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::change) { isEqualTo(lastChange) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                expectThat(it[2]).isA<AddRelationChange>()
                    .with(AddRelationChange::from) { isEqualTo("firstUserName") }
                    .with(AddRelationChange::to) { isEqualTo("firstGroup") }
            }

            askAllForChanges(apps.getPeers(1).values).forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(secondChange)
                expectThat(it[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::change) { isEqualTo(lastChange) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                expectThat(it[2]).isA<AddRelationChange>()
                    .with(AddRelationChange::from) { isEqualTo("firstUserName") }
                    .with(AddRelationChange::to) { isEqualTo("firstGroup") }
            }
        }

    private suspend fun executeChange(uri: String, change: Change): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peer: PeerAddress) =
        testHttpClient.get<Changes>("http://${peer.address}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { askForChanges(it) }

    private fun change(vararg peersetIds: Int) = AddUserChange(
        "userName",
        peersets = peersetIds.map {
            ChangePeersetInfo(it, InitialHistoryEntry.getId())
        },
    )

    private fun change(vararg peersetToChangeId: Pair<Int, String>) = AddUserChange(
        "userName",
        peersets = peersetToChangeId.map {
            ChangePeersetInfo(it.first, it.second)
        },
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}
