package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.common.*
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
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.time.Duration
import java.util.concurrent.Phaser

@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class MultiplePeersetSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(MultiplePeersetSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {
        val phaser = Phaser(6)
        phaser.register()
        val electionPhaser = Phaser(4)
        electionPhaser.register()
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyEnd to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                phaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()
        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.values).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
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
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.ReachedMaxRetries to peerReachedMaxRetries,
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            appsToExclude = listOf(3, 4, 5),
            signalListeners = (0..5).associateWith { signalListenersForCohort },
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val change: Change = change(0, 1)

        val result = executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", change)

        expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

        maxRetriesPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should not be executed
        askAllForChanges(apps.getPeers(0).values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }

        try {
            testHttpClient.get<HttpResponse>(
                "http://${apps.getPeer(0, 0).address}/v2/change_status/${change.id}"
            ) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }
            fail("executing change didn't fail")
        } catch (e: ServerResponseException) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
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
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)
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
        val phaser = Phaser(5)
        phaser.register()

        val peerApplyCommitted = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            phaser.arrive()
        }


        val electionPhaser = Phaser(3)
        electionPhaser.register()
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyCommitted to peerApplyCommitted,
            Signal.ConsensusLeaderElected to leaderElected
        )

        apps = TestApplicationSet(
            listOf(3, 5),
            appsToExclude = listOf(2, 6, 7),
            signalListeners = (0..7).associateWith { signalListenersForCohort },
        )
        val peers = apps.getPeers()
        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)


        phaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should be executed in every peerset
        askAllForChanges(peers.values.filter { it.address != NON_RUNNING_PEER })
            .forEach { changes ->
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change)
            }
    }

    @Test
    fun `transaction should not be processed if every peer from one peerset fails after ft-agree`(): Unit =
        runBlocking {

            val failAction = SignalListener {
                throw RuntimeException("Every peer from one peerset fails")
            }
            apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = (3..7).associateWith { mapOf(Signal.OnHandlingAgreeEnd to failAction) },
            )
            val peers = apps.getPeers()
            val change = change(0, 1)

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too few responses of ft phase.")
            }

            askAllForChanges(peers.values).forEach { changes ->
                expectThat(changes.size).isEqualTo(0)
            }
        }

    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit = runBlocking {
        val failAction = SignalListener {
            throw RuntimeException("Leader failed after ft-agree")
        }

        val applyCommittedPhaser = Phaser(8)
        applyCommittedPhaser.register()

        val peerApplyCommitted = SignalListener {
            logger.info("Arrived: ${it.subject.getPeerName()}")
            applyCommittedPhaser.arrive()
        }

        val signalListenersForLeaders = mapOf(
            Signal.BeforeSendingApply to failAction,
            Signal.OnHandlingApplyCommitted to peerApplyCommitted,
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
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)
        }.subject.let { e ->
            // TODO rewrite — we cannot model leader failure as part of API
            expect {
                that(e.response.status).isEqualTo(HttpStatusCode.InternalServerError)
                that(e.response.readText()).contains("Change not applied due to timeout")
                that(e.response.readText()).contains("Leader failed after ft-agree")
            }
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peers.values).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {
            val consensusLeaderElectedPhaser = Phaser(6)
            val changePhaser = Phaser(8)
            listOf(changePhaser, consensusLeaderElectedPhaser).forEach { it.register() }


            val leaderAction = SignalListener {
                val url2 = "${it.peers[0][1]}/apply"
                runBlocking {
                    httpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT,
                            change(0, 1),
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
                            change(0, 1),
                        )
                    }.also {
                        logger.info("Got response test apply ${it.status.value}")
                    }
                }
                logger.info("${it.peers[0][2]} sent response to apply")
                throw RuntimeException("Leader failed after applying change in one peerset")
            }

            val signalListenersForAll = mapOf(
                Signal.OnHandlingApplyCommitted to SignalListener {
                    logger.info("Arrived on apply ${it.subject.getPeerName()}")
                    changePhaser.arrive()
                },
                Signal.ConsensusLeaderElected to SignalListener {
                    consensusLeaderElectedPhaser.arrive()
                }
            )
            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to leaderAction,
            )

            apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = mapOf(
                    0 to signalListenersForAll + signalListenersForLeader,
                    1 to signalListenersForAll,
                    2 to signalListenersForAll,
                    3 to signalListenersForAll,
                    4 to signalListenersForAll,
                    5 to signalListenersForAll,
                    6 to signalListenersForAll,
                    7 to signalListenersForAll,
                )
            )
            val peers = apps.getPeers()
            val change = change(0, 1)

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChange("http://${apps.getPeer(0, 0).address}/v2/change/sync", change)
            }

            changePhaser.arriveAndAwaitAdvanceWithTimeout()

            // waiting for consensus to propagate change is waste of time and fails CI
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
            val finalChangePhaser = Phaser(8)

            listOf(firstChangePhaser, secondChangePhaser, finalChangePhaser, consensusLeaderElectedPhaser)
                .forEach { it.register() }

            val firstChangeListener = SignalListener {
                if (it.change!! is AddUserChange) {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    firstChangePhaser.arrive()
                }
            }

            val secondChangeListener = SignalListener {
                if (it.change!! is AddGroupChange) {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    secondChangePhaser.arrive()
                }
            }

            val finalChangeListener = SignalListener {
                if (it.change is AddRelationChange) {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    finalChangePhaser.arrive()
                }
            }

            val leaderElectedListener = SignalListener {
                consensusLeaderElectedPhaser.arrive()
            }


//          Await to elect leader in consensus

            apps = TestApplicationSet(
                listOf(3, 5),
                signalListeners = List(3) {
                    it to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to firstChangeListener,
                        Signal.OnHandlingApplyEnd to finalChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap()
                        + List(5) {
                    it + 3 to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to secondChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                        Signal.OnHandlingApplyEnd to finalChangeListener
                    )
                }.toMap()
            )
            val peers = apps.getPeers()

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(15))

            // given - change in first peerset
            expectCatching {
                executeChange(
                    "http://${apps.getPeer(0, 0).address}/v2/change/sync", AddUserChange(
                        listOf(
                            ChangePeersetInfo(0, InitialHistoryEntry.getId()),
                        ),
                        "firstUserName",
                    )
                )
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            // and - change in second peerset
            expectCatching {
                executeChange(
                    "http://${apps.getPeer(1, 0).address}/v2/change/sync",
                    AddGroupChange(
                        listOf(
                            ChangePeersetInfo(1, InitialHistoryEntry.getId()),
                        ),
                        "firstGroup",
                    )
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            val lastChange0 = askForChanges(apps.getPeer(0, 0)).last()
            val lastChange1 = askForChanges(apps.getPeer(1, 0)).last()

            // when - executing change between two peersets
            val addRelationChange = AddRelationChange(
                listOf(
                    ChangePeersetInfo(0, lastChange0.toHistoryEntry(0).getId()),
                    ChangePeersetInfo(1, lastChange1.toHistoryEntry(1).getId()),
                ),
                "firstUserName",
                "firstGroup",
            )

            expectCatching {
                executeChange(
                    "http://${apps.getPeer(0, 0).address}/v2/change/sync",
                    addRelationChange
                )
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            askAllForChanges(peers.values).let {
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

    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { askForChanges(it) }

    private fun change(vararg peersetIds: Int) = AddUserChange(
        peersetIds.map {
            ChangePeersetInfo(it, InitialHistoryEntry.getId())
        },
        "userName",
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}
