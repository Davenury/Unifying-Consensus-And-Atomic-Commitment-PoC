package com.github.davenury.ucac.api

import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.history.InitialHistoryEntry
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
import java.util.concurrent.Phaser
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
        val phaser = Phaser(6)
        phaser.register()
        val electionPhaser = Phaser(4)
        electionPhaser.register()
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyEnd to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                phaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected
        )

        val apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()
        val change = change(peers[1][0])

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change)

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.flatten()).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }

        apps.stopApps()
    }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit = runBlocking {
        val maxRetriesPhaser = Phaser(1)
        maxRetriesPhaser.register()
        val peerReachedMaxRetries = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            maxRetriesPhaser.arrive()
        }

        val electionPhaser = Phaser(2)
        electionPhaser.register()
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.ReachedMaxRetries to peerReachedMaxRetries,
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

        val result = executeChange("http://${peers[0][0]}/v2/change/async", change)
        change = change.withAddress(peers[0][0])

        expectThat(result.status).isEqualTo(HttpStatusCode.Created)

        maxRetriesPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should not be executed
        askAllForChanges(peers[0]).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
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
        } catch (e: ServerResponseException) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
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
            executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change(otherPeer))
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
        val phaser = Phaser(5)
        phaser.register()

        val peerApplyCommitted = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            phaser.arrive()
        }


        val electionPhaser = Phaser(3)
        electionPhaser.register()
        val leaderElected = SignalListener {
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyCommitted to peerApplyCommitted,
            Signal.ConsensusLeaderElected to leaderElected
        )

        val apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 6, 7),
            signalListeners = (0..7).associateWith { signalListenersForCohort },
        )
        val peers = apps.getPeers()
        val change = change(apps.getPeers()[1][0])

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change)


        phaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should be executed in every peerset
        askAllForChanges(peers.flatten() subtract setOf(NON_RUNNING_PEER))
            .forEach { changes ->
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change)
            }

        apps.stopApps()
    }

    @Test
    fun `transaction should not be processed if every peer from one peerset fails after ft-agree`(): Unit =
        runBlocking {

            val failAction = SignalListener {
                throw RuntimeException("Every peer from one peerset fails")
            }
            val apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = (3..7).associateWith { mapOf(Signal.OnHandlingAgreeEnd to failAction) },
            )
            val peers = apps.getPeers()
            val otherPeer = apps.getPeers()[1][0]

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change(otherPeer))
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too few responses of ft phase.")
            }

            askAllForChanges(peers.flatten()).forEach { changes ->
                expectThat(changes.size).isEqualTo(0)
            }

            apps.stopApps()
        }

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
            executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change(otherPeer))
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.flatten()).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change(otherPeer))
        }

        apps.stopApps()
    }

    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {
            val phaser = Phaser(8)
            phaser.register()

            val leaderAction = SignalListener {
                val url2 = "${it.peers[0][1]}/apply"
                runBlocking {
                    httpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT,
                            change(it.peers[1][0]),
                        )
                    }.also {
                        logger.info("Got response test apply ${it.status.value}")
                    }
                }
                logger.info("${it.peers[0][1]} sent response to apply")
                val url3 = "${it.peers[0][2]}/apply"
                runBlocking {
                    httpClient.post<HttpResponse>(url3) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT,
                            change(it.peers[1][0]),
                        )
                    }.also {
                        logger.info("Got response test apply ${it.status.value}")
                    }
                }
                logger.info("${it.peers[0][2]} sent response to apply")
                throw RuntimeException("Leader failed after applying change in one peerset")
            }

            val arrivalCount = AtomicInteger(0)
            val peerApplyEnd = SignalListener {
                arrivalCount.incrementAndGet()
                phaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.OnHandlingApplyCommitted to peerApplyEnd,
            )
            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to leaderAction,
            ) + signalListenersForCohort

            val apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = mapOf(
                    0 to signalListenersForLeader,
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
                executeChange("http://${peers[0][0]}/v2/change/sync?use_2pc", change(otherPeer))
            }

            phaser.arriveAndAwaitAdvanceWithTimeout()

            // waiting for consensus to propagate change is waste of time and fails CI
            askAllForChanges(peers.flatten()).forEach { changes ->
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change(otherPeer) as Change)
            }

            apps.stopApps()
        }

    @Test
    fun `should be able to execute change in two different peersets even if changes in peersets are different`() =
        runBlocking {
            val firstChangePhaser = Phaser(3)
            firstChangePhaser.register()
            val secondChangePhaser = Phaser(5)
            secondChangePhaser.register()
            val finalChangePhaser = Phaser(8)
            finalChangePhaser.register()

            val firstPeersetListener = SignalListener {
                if (it.change!! is AddUserChange) {
                    firstChangePhaser.arrive()
                } else if (it.change is AddRelationChange) {
                    finalChangePhaser.arrive()
                }
            }

            val secondPeersetListener = SignalListener {
                if (it.change!! is AddGroupChange) {
                    secondChangePhaser.arrive()
                } else if (it.change is AddRelationChange) {
                    finalChangePhaser.arrive()
                }
            }

            val apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = List(3) { it to mapOf(Signal.OnHandlingApplyEnd to firstPeersetListener) }.toMap()
                        + List(5) { it + 3 to mapOf(Signal.OnHandlingApplyEnd to secondPeersetListener) }.toMap()
            )
            val peers = apps.getPeers()

            // given - change in first peerset
            val someChange = AddUserChange(InitialHistoryEntry.getId(), "firstUserName", listOf())
            expectCatching {
                executeChange("http://${peers[0][0]}/v2/change/sync", someChange)
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvance()

            // and - change in second peerset
            expectCatching {
                executeChange(
                    "http://${peers[1][0]}/v2/change/sync",
                    AddGroupChange(InitialHistoryEntry.getId(), "firstGroup", listOf())
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvance()

            val lastChange = askForChanges(peers[0][0]).last()

            // when - executing change between two peersets
            val addRelationChange = AddRelationChange(
                lastChange.toHistoryEntry().getId(),
                "firstUserName",
                "firstGroup",
                listOf(peers[1][0])
            )

            expectCatching {
                executeChange("http://${peers[0][0]}/v2/change/sync", addRelationChange)
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvance()

            askAllForChanges(peers.flatten()).let {
                it.forEach {
                    (it.last() as AddRelationChange).let {
                        expectThat(it.from).isEqualTo(addRelationChange.from)
                        expectThat(it.to).isEqualTo(addRelationChange.to)
                    }
                }
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

    private fun enrichChange(change: Change, peer: String) =
        change.withAddress(peer)

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}
