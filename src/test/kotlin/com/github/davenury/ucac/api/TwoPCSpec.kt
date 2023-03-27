package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.common.PeerAddress
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
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis

@Disabled("Temporary")
@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class TwoPCSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(TwoPCSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
        )

        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isA<TwoPCChange>()
                .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                .with(TwoPCChange::change) { isEqualTo(change) }
            expectThat(changes[1]).isA<AddUserChange>()
                .with(AddUserChange::userName) { isEqualTo("userName") }
        }
    }


    @Test
    fun `1000 change processed sequentially`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val leaderElectedPhaser = Phaser(peersWithoutLeader)
        leaderElectedPhaser.register()

        val phaser = Phaser(peersWithoutLeader * 2)
        phaser.register()


        val peerLeaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            leaderElectedPhaser.arrive()
        }

        val peerChangeAccepted = SignalListener {
            logger.info("Arrived change: ${it.change}")
            phaser.arrive()
        }


        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeAccepted to peerChangeAccepted
                )
            }
        )
        val peerAddresses = apps.getPeerAddresses("peerset0")

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")


        var change = change(0, 1)

        val endRange = 1000

        var time = 0L

        repeat(endRange) {
            time += measureTimeMillis {
                expectCatching {
                    executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change)
                }.isSuccess()
            }
            phaser.arriveAndAwaitAdvanceWithTimeout()
            change = twoPeersetChange(change)
        }
        // when: peer1 executed change

        expectThat(time / endRange).isLessThanOrEqualTo(500L)

        askAllForChanges(peerAddresses.values).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(endRange * 2)
            expectThat(changes.all { it is TwoPCChange && it.twoPCStatus == TwoPCStatus.ACCEPTED || it is AddUserChange }).isTrue()

        }
    }

    @Test
    fun `should be able to execute 2 transactions`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(4)
        val changeSecondAppliedPhaser = Phaser(4)
        val electionPhaser = Phaser(4)
        listOf(changeAppliedPhaser, changeSecondAppliedPhaser, electionPhaser).forEach { it.register() }

        val change = change(0, 1)

        val historyEntryId = { peersetNum: Int ->
            change.toHistoryEntry(PeersetId("peerset$peersetNum")).getId()
        }

        val changeSecond = change(Pair(0, historyEntryId(0)), Pair(1, historyEntryId(1)))

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (it.change!!.id == change.id) changeAppliedPhaser.arrive()
                if (it.change!!.id == changeSecond.id) changeSecondAppliedPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() }
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
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?use_2pc=true", changeSecond)

        changeSecondAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
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
        val change2PCAppliedPhaser = Phaser(1)
        val changeRaftAppliedPhaser = Phaser(2)
        val applied2PCChangesListener = SignalListener {
            logger.info("Arrived 2PC: ${it.subject.getPeerName()}")
            change2PCAppliedPhaser.arrive()
        }

        val appliedRaftChangesListener = SignalListener {
            if (it.change is TwoPCChange && (it.change as TwoPCChange).twoPCStatus == TwoPCStatus.ABORTED) {
                logger.info("Arrived raft: ${it.subject.getPeerName()}")
                changeRaftAppliedPhaser.arrive()
            }
        }

        val electionPhaser = Phaser(2)
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        listOf(electionPhaser, change2PCAppliedPhaser, changeRaftAppliedPhaser).forEach { it.register() }

        val signalListenersForCohort = mapOf(
            Signal.TwoPCOnChangeApplied to applied2PCChangesListener,
            Signal.ConsensusFollowerChangeAccepted to appliedRaftChangesListener,
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            appsToExclude = listOf("peer3", "peer4", "peer5"),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort },
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val change: Change = change(0, 1)
        val result = executeChange("http://${apps.getPeer("peer0").address}/v2/change/async?use_2pc=true", change)

        expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

        change2PCAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()
        changeRaftAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()


        // then - transaction should not be executed
        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
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
                "http://${apps.getPeer("peer0").address}/v2/change_status/${change.id}"
            ) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }
            fail("executing change didn't fail")
        } catch (e: ClientRequestException) {
            expectThat(e.response.status).isEqualTo(HttpStatusCode.NotFound)
        }
    }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
            ),
            appsToExclude = listOf("peer2", "peer5", "peer6", "peer7"),
        )
        val change = change(0, 1)

        delay(5000)

        // when - executing transaction
        try {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change)
            fail("Exception not thrown")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
            ),
            appsToExclude = listOf("peer2", "peer6", "peer7"),
            signalListeners = (0..7).map { "peer$it" }.associateWith { signalListenersForCohort },
        )
        val peers = apps.getPeerAddresses()
        val change: Change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change)

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
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                ),
                signalListeners =
                (0..2).map { "peer$it" }.associateWith {
                    mapOf(
                        Signal.ConsensusLeaderElected to leaderElected,
                        Signal.ConsensusFollowerChangeAccepted to SignalListener {
                            logger.info("Change accepted (first): ${it.subject}")
                            firstPeersetChangeAppliedPhaser.arrive()
                        }
                    )
                }.toMap() + (3..7).map { "peer$it" }.associateWith {
                    mapOf(
                        Signal.TwoPCOnHandleDecision to onHandleDecision,
                        Signal.ConsensusLeaderElected to leaderElected,
                        Signal.ConsensusFollowerChangeAccepted to SignalListener {
                            logger.info("Change accepted (second): ${it.subject}")
                            secondPeersetChangeAppliedPhaser.arrive()
                        }
                    )
                }.toMap(),
            )
            val peers = apps.getPeerAddresses()
            val change: Change = change(0, 1)

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change)

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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
            ),
            signalListeners = mapOf(
                "peer0" to signalListenersForLeaders,
                "peer1" to signalListenersForCohort,
                "peer2" to signalListenersForCohort,
                "peer3" to signalListenersForCohort,
                "peer4" to signalListenersForCohort,
                "peer5" to signalListenersForCohort,
                "peer6" to signalListenersForCohort,
                "peer7" to signalListenersForCohort,
            )
        )
        val change = change(0, 1)

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change)
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
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
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                ),
                signalListeners = List(3) {
                    "peer$it" to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to firstChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap() + List(5) {
                    "peer${it + 3}" to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to secondChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap()
            )

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // given - change in first peerset
            val firstChange = AddUserChange(
                "firstUserName",
                peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId())),
            )
            expectCatching {
                executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", firstChange)
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // and - change in second peerset
            val secondChange = AddGroupChange(
                "firstGroup",
                peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), InitialHistoryEntry.getId())),
            )
            expectCatching {
                executeChange(
                    "http://${apps.getPeer("peer3").address}/v2/change/sync",
                    secondChange
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing change between two peersets
            val lastChange: Change = AddRelationChange(
                "firstUserName",
                "firstGroup",
                peersets = listOf(
                    ChangePeersetInfo(PeersetId("peerset0"), firstChange.toHistoryEntry(PeersetId("peerset0")).getId()),
                    ChangePeersetInfo(
                        PeersetId("peerset1"),
                        secondChange.toHistoryEntry(PeersetId("peerset1")).getId()
                    ),
                ),
            )

            expectCatching {
                executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", lastChange)
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout()

//          First peerset
            askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(firstChange)
                expectThat(it[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::change) { isEqualTo(lastChange) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                expectThat(it[2]).isA<AddRelationChange>()
                    .with(AddRelationChange::from) { isEqualTo("firstUserName") }
                    .with(AddRelationChange::to) { isEqualTo("firstGroup") }
            }

            askAllForChanges(apps.getPeerAddresses("peerset1").values).forEach {
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

    @Test
    fun `atomic commitment between one-peer peersets`(): Unit = runBlocking {
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0"),
                "peerset1" to listOf("peer1"),
            ),
        )

        expectCatching {
            val change1 = change(0, 1)
            val change2 = twoPeersetChange(change1)
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change1)
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync?use_2pc=true", change2)
        }.isSuccess()

        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
            expectThat(changes.size).isEqualTo(4)
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

    private fun change(vararg peersetNums: Int) = AddUserChange(
        "userName",
        peersets = peersetNums.map {
            ChangePeersetInfo(PeersetId("peerset$it"), InitialHistoryEntry.getId())
        },
    )

    private fun change(vararg peersetToChangeId: Pair<Int, String>) = AddUserChange(
        "userName",
        peersets = peersetToChangeId.map {
            ChangePeersetInfo(PeersetId("peerset${it.first}"), it.second)
        },
    )

    private fun twoPeersetChange(
        change: Change
    ) = AddUserChange(
        "userName",
        peersets = (0..1).map { PeersetId("peerset$it") }
            .map { ChangePeersetInfo(it, change.toHistoryEntry(it).getId()) },
    )
}
