package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestApplicationSet.Companion.NON_RUNNING_PEER
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@Suppress("HttpUrlsUsage")
class TwoPCSpec {
    companion object {
        private val logger = LoggerFactory.getLogger(TwoPCSpec::class.java)
    }

    @BeforeEach
    fun setup(testInfo: TestInfo) {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
        println("\n\n${testInfo.displayName}")
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {
        val changeAppliedPhaser = Phaser(8)
        val electionPhaser = Phaser(4)
        listOf(changeAppliedPhaser, electionPhaser).forEach { it.register() }

        val signalListenersForCohort = mapOf(
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                println("Change accepted ${it.subject.getPeerName()} ${it.change}")
                changeAppliedPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() }
        )

        val apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()
        var change = change(peers[1][0])

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/v2/change/async?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        change = change.copy(peers = listOf(peers[1][0], peers[0][0]))

        val twoPCChange =
            TwoPCChange(
                change.parentId,
                change.peers,
                change.acceptNum,
                twoPCStatus = TwoPCStatus.ACCEPTED,
                change = change
            )

        askAllForChanges(peers.flatten()).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isEqualTo(twoPCChange)
            expectThat(changes[1]).isEqualTo(change.copyWithNewParentId(twoPCChange.toHistoryEntry().getId()))
        }

        apps.stopApps()
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

        val apps = TestApplicationSet(
            listOf(3, 3),
            appsToExclude = listOf(3, 4, 5),
            signalListeners = (0..5).associateWith { signalListenersForCohort },
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val peers = apps.getPeers()
        val otherPeer = peers[1][0]
        var change: Change = change(otherPeer)

        val result = executeChange("http://${peers[0][0]}/v2/change/async?use_2pc=True", change)
        change = change.withAddress(peers[0][0])
        val first2PCChange =
            TwoPCChange(change.parentId, change.peers, twoPCStatus = TwoPCStatus.ACCEPTED, change = change)
        val second2PCChange =
            TwoPCChange(
                first2PCChange.toHistoryEntry().getId(),
                change.peers,
                twoPCStatus = TwoPCStatus.ABORTED,
                change = change
            )

        expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should not be executed
        askAllForChanges(peers[0]).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isEqualTo(first2PCChange)
            expectThat(changes[1]).isEqualTo(second2PCChange)
        }


        try {
            val response: HttpResponse = testHttpClient.get(
                "http://${peers[0][0]}/v2/change_status/${
                    change.toHistoryEntry().getId()
                }"
            ) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }
            fail("executing change didn't fail")
        } catch (e: ClientRequestException) {
            expectThat(e.message!!).contains("Change conflicted")
        }
        apps.stopApps()
    }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {
        val apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 5, 6, 7),
        )
        val peers = apps.getPeers()
        val otherPeer = apps.getPeers()[1][0]

        delay(5000)

        // when - executing transaction
        try {
            executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc=true", change(otherPeer))
            fail("Exception not thrown")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askAllForChanges(peers[0]).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }

        apps.stopApps()
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

        val apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 6, 7),
            signalListeners = (0..7).associateWith { signalListenersForCohort },
        )
        val peers = apps.getPeers()
        var change: Change = change(apps.getPeers()[1][0])

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc=true", change)

        changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

        change = change.withAddress(peers[0][0])
        val twoPCChange =
            TwoPCChange(change.parentId, change.peers, twoPCStatus = TwoPCStatus.ACCEPTED, change = change)

        // then - transaction should be executed in every peerset
        askAllForChanges(peers.flatten() subtract setOf(NON_RUNNING_PEER)).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
            expectThat(changes[0]).isEqualTo(twoPCChange)
            expectThat(changes[1]).isEqualTo(change.copyWithNewParentId(twoPCChange.toHistoryEntry().getId()))
        }
        apps.stopApps()
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

            var isChangeNotAccepted:AtomicBoolean = AtomicBoolean(true)

            val onHandleDecision = SignalListener {
                if (isChangeNotAccepted.get()) throw Exception("Simulate ignoring 2PC-decision message")
            }

            val apps = TestApplicationSet(
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
            val otherPeer = apps.getPeers()[1][0]
            var change: Change = change(otherPeer)


            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc=True", change)

            firstPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            isChangeNotAccepted.set(false)

            secondPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            change = change.withAddress(peers[0][0])
            val twoPCChange =
                TwoPCChange(change.parentId, change.peers, twoPCStatus = TwoPCStatus.ACCEPTED, change = change)

            askAllForChanges(peers.flatten()).forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isEqualTo(twoPCChange)
                expectThat(changes[1]).isEqualTo(change.copyWithNewParentId(twoPCChange.toHistoryEntry().getId()))
            }

            apps.stopApps()
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

        val apps = TestApplicationSet(
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
        val otherPeer = apps.getPeers()[1][0]

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc=True", change(otherPeer))
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.flatten()).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change(otherPeer))
        }

        apps.stopApps()
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


//          Await to elect leader in consensus

            val apps = TestApplicationSet(
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
            val peers = apps.getPeers()

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // given - change in first peerset
            val firstChange = AddUserChange(InitialHistoryEntry.getId(), "firstUserName", listOf())
            expectCatching {
                executeChange("http://${peers[0][0]}/v2/change/sync", firstChange)
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // and - change in second peerset
            val secondChange = AddGroupChange(InitialHistoryEntry.getId(), "firstGroup", listOf())
            expectCatching {
                executeChange(
                    "http://${peers[1][0]}/v2/change/sync",
                    secondChange
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing change between two peersets
            var lastChange: Change = AddRelationChange(
                firstChange.toHistoryEntry().getId(),
                "firstUserName",
                "firstGroup",
                listOf(peers[1][0])
            )

            expectCatching {
                executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc=True", lastChange)
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            lastChange = lastChange.withAddress(peers[0][0])
            val twoPCChange =
                TwoPCChange(
                    lastChange.parentId,
                    lastChange.peers,
                    twoPCStatus = TwoPCStatus.ACCEPTED,
                    change = lastChange
                )

//          First peerset
            askAllForChanges(peers[0]).forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(firstChange)
                expectThat(it[1]).isEqualTo(twoPCChange)
                expectThat(it[2]).isEqualTo(lastChange.copyWithNewParentId(twoPCChange.toHistoryEntry().getId()))
            }

            askAllForChanges(peers[1]).forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(secondChange)
                val updatedTwoPCChange = twoPCChange.copyWithNewParentId(secondChange.toHistoryEntry().getId())
                expectThat(it[1]).isEqualTo(updatedTwoPCChange)
                val updatedLastChange = lastChange.copyWithNewParentId(updatedTwoPCChange.toHistoryEntry().getId())
                expectThat(it[2]).isEqualTo(updatedLastChange)
            }
        }

    private suspend fun executeChange(uri: String, change: Change): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peer: String) =
        testHttpClient.get<Changes>("http://$peer/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<String>) =
        peerAddresses.map { askForChanges(it) }

    private fun change(otherPeersetPeer: String) = AddUserChange(
        InitialHistoryEntry.getId(),
        "userName",
        // leader should enrich himself
        listOf(otherPeersetPeer)
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}
