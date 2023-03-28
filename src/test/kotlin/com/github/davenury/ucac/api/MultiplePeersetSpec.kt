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
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.time.Duration
import java.util.concurrent.Phaser
import kotlin.system.measureTimeMillis

@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class MultiplePeersetSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(MultiplePeersetSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort }
        )

        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }


    @Test
    fun `1000 change processed sequentially`(): Unit = runBlocking {
        val phaser = Phaser(6)
        var change = change(0, 1)
        phaser.register()

        val peersWithoutLeader = 4
        val leaderElectionPhaser = Phaser(peersWithoutLeader)
        leaderElectionPhaser.register()

        val peerLeaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            leaderElectionPhaser.arrive()
        }

        val endRange = 1000

        val changeAccepted = SignalListener {
            logger.info("Arrived change: ${it.change}")
            if (change.id == it.change?.id) phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.OnHandlingApplyEnd to changeAccepted,
                    Signal.ConsensusFollowerChangeAccepted to changeAccepted,
                )
            }
        )

        leaderElectionPhaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")

        var time = 0L

        repeat((0 until endRange).count()) {
            time += measureTimeMillis {
                expectCatching {
                    executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
                }.isSuccess()
            }
            phaser.arriveAndAwaitAdvanceWithTimeout()
            change = twoPeersetChange(change)
        }
        // when: peer1 executed change

        expectThat(time / endRange).isLessThanOrEqualTo(500L)

        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(endRange)
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            appsToExclude = listOf("peer3", "peer4", "peer5"),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort },
        )

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val change: Change = change(0, 1)

        val result = executeChange("http://${apps.getPeer("peer0").address}/v2/change/async", change)

        expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

        maxRetriesPhaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should not be executed
        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }

        try {
            testHttpClient.get<HttpResponse>(
                "http://${apps.getPeer("peer0").address}/v2/change_status/${change.id}"
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
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
            ),
            appsToExclude = listOf("peer2", "peer6", "peer7"),
            signalListeners = (0..7).map { "peer$it" }.associateWith { signalListenersForCohort },
        )
        val change = change(0, 1)

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)


        phaser.arriveAndAwaitAdvanceWithTimeout()

        // then - transaction should be executed in every peerset
        askAllForChanges(apps.getPeerAddresses().values.filter { it.address != NON_RUNNING_PEER })
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
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                ),
                signalListeners = (3..7).map { "peer$it" }
                    .associateWith { mapOf(Signal.OnHandlingAgreeEnd to failAction) },
                configOverrides = (0..2).map { "peer$it" }.associateWith {
                    mapOf(
                        "gpac.responsesTimeouts.agreeTimeout" to Duration.ZERO,
                        "gpac.ftAgreeRepeatDelay" to Duration.ZERO,
                        "gpac.leaderFailDelay" to Duration.ZERO
                    )
                }
            )
            val change = change(0, 1)

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too few responses of ft phase.")
            }

            askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
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
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
        }.subject.let { e ->
            // TODO rewrite — we cannot model leader failure as part of API
            expect {
                that(e.response.status).isEqualTo(HttpStatusCode.InternalServerError)
                that(e.response.readText()).contains("Change not applied due to timeout")
                that(e.response.readText()).contains("Leader failed after ft-agree")
            }
        }

        applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {
            val changePhaser = Phaser(8)
            changePhaser.register()


            val leaderAction = SignalListener { data ->
                val url2 = "${data.peerResolver.resolve("peer1")}/apply"
                runBlocking {
                    httpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            data.transaction!!.ballotNumber, true, Accept.COMMIT,
                            change(0, 1),
                        )
                    }.also {
                        logger.info("Got response test apply ${it.status.value}")
                    }
                }
                logger.info("${data.peerResolver.resolve("peer1")} sent response to apply")
                val url3 = "${data.peerResolver.resolve("peer2")}/apply"
                runBlocking {
                    httpClient.post<HttpResponse>(url3) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            data.transaction!!.ballotNumber, true, Accept.COMMIT,
                            change(0, 1),
                        )
                    }.also {
                        logger.info("Got response test apply ${it.status.value}")
                    }
                }
                logger.info("${data.peerResolver.resolve("peer2")} sent response to apply")
                throw RuntimeException("Leader failed after applying change in one peerset")
            }

            val signalListenersForAll = mapOf(
                Signal.OnHandlingApplyCommitted to SignalListener {
                    logger.info("Arrived on apply ${it.subject.getPeerName()}")
                    changePhaser.arrive()
                },
            )
            val signalListenersForLeader = mapOf(
                Signal.BeforeSendingApply to leaderAction,
            )

            apps = TestApplicationSet(
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                ),
                signalListeners = mapOf(
                    "peer0" to signalListenersForAll + signalListenersForLeader,
                    "peer1" to signalListenersForAll,
                    "peer2" to signalListenersForAll,
                    "peer3" to signalListenersForAll,
                    "peer4" to signalListenersForAll,
                    "peer5" to signalListenersForAll,
                    "peer6" to signalListenersForAll,
                    "peer7" to signalListenersForAll,
                ),
                configOverrides = mapOf(
                    "peer0" to mapOf("raft.isEnabled" to false),
                    "peer1" to mapOf("raft.isEnabled" to false),
                    "peer2" to mapOf("raft.isEnabled" to false),
                    "peer3" to mapOf("raft.isEnabled" to false),
                    "peer4" to mapOf("raft.isEnabled" to false),
                    "peer5" to mapOf("raft.isEnabled" to false),
                    "peer6" to mapOf("raft.isEnabled" to false),
                    "peer7" to mapOf("raft.isEnabled" to false),
                ),
            )
            val change = change(0, 1)

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
            }

            changePhaser.arriveAndAwaitAdvanceWithTimeout()

            // waiting for consensus to propagate change is waste of time and fails CI
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
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2"),
                    "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                ),
                signalListeners = List(3) {
                    "peer$it" to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to firstChangeListener,
                        Signal.OnHandlingApplyEnd to finalChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                    )
                }.toMap() + List(5) {
                    "peer${it + 3}" to mapOf(
                        Signal.ConsensusFollowerChangeAccepted to secondChangeListener,
                        Signal.ConsensusLeaderElected to leaderElectedListener,
                        Signal.OnHandlingApplyEnd to finalChangeListener
                    )
                }.toMap()
            )

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // given - change in first peerset
            expectCatching {
                executeChange(
                    "http://${apps.getPeer("peer0").address}/v2/change/sync", AddUserChange(
                        "firstUserName",
                        peersets = listOf(
                            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
                        ),
                    )
                )
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // and - change in second peerset
            expectCatching {
                executeChange(
                    "http://${apps.getPeer("peer3").address}/v2/change/sync",
                    AddGroupChange(
                        "firstGroup",
                        peersets = listOf(
                            ChangePeersetInfo(PeersetId("peerset1"), InitialHistoryEntry.getId()),
                        ),
                    )
                )
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            val lastChange0 = askForChanges(apps.getPeer("peer0")).last()
            val lastChange1 = askForChanges(apps.getPeer("peer3")).last()

            // when - executing change between two peersets
            val addRelationChange = AddRelationChange(
                "firstUserName",
                "firstGroup",
                peersets = listOf(
                    ChangePeersetInfo(PeersetId("peerset0"), lastChange0.toHistoryEntry(PeersetId("peerset0")).getId()),
                    ChangePeersetInfo(PeersetId("peerset1"), lastChange1.toHistoryEntry(PeersetId("peerset1")).getId()),
                ),
            )

            expectCatching {
                executeChange(
                    "http://${apps.getPeer("peer0").address}/v2/change/sync",
                    addRelationChange
                )
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges(apps.getPeerAddresses().values).let {
                it.forEach {
                    (it.last() as AddRelationChange).let {
                        expectThat(it.from).isEqualTo(addRelationChange.from)
                        expectThat(it.to).isEqualTo(addRelationChange.to)
                    }
                }
            }
        }

    @Test
    fun `should commit change if super-set agrees to commit`(): Unit = runBlocking {
        val electSignal = mapOf(
            Signal.OnHandlingElectBegin to SignalListener {
                throw RuntimeException("Should not respond to elect me")
            }
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            signalListeners = mapOf(
                "peer2" to electSignal,
                "peer5" to electSignal,
            ),
            configOverrides = (0..5).map { "peer$it" }.associateWith { mapOf("raft.isEnabled" to false) }
        )

        val change: Change = change(0, 1)

        expectCatching {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
        }.isSuccess()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(1)
        }
    }

    @Test
    fun `should commit change if super-set agrees to commit, even though someone yells abort`(): Unit = runBlocking {
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            configOverrides = (0..5).map { "peer$it" }.associateWith {
                mapOf("raft.isEnabled" to false)
            } + mapOf("peer2" to mapOf("gpac.abortOnElectMe" to true))
        )

        val change: Change = change(0, 1)

        expectCatching {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change)
        }.isSuccess()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(1)
        }
    }

    @Test
    fun `should abort change if super-set decides so, even though some peers agree`(): Unit = runBlocking {
        val phaser = Phaser(7)
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            configOverrides = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    "raft.isEnabled" to false,
                    "gpac.abortOnElectMe" to true
                )
            } + mapOf(
                "peer0" to mapOf("gpac.abortOnElectMe" to false),
                "peer5" to mapOf("gpac.abortOnElectMe" to false),
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith {
                mapOf(Signal.OnHandlingApplyEnd to SignalListener { phaser.arrive() })
            }
        )

        val change: Change = change(0, 1)

        expectCatching {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async", change)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }
    }

    @Test
    fun `should repeat change change if peersets do not agree`(): Unit = runBlocking {
        val phaser = Phaser(2)
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            configOverrides = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    "raft.isEnabled" to false,
                )
            } + (3..5).map { "peer$it" }.associateWith { mapOf("gpac.abortOnElectMe" to true) }
                    + mapOf(
                "peer0" to mapOf(
                    "gpac.initialRetriesDelay" to Duration.ZERO,
                    "gpac.retriesBackoffTimeout" to Duration.ZERO
                )
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.OnHandlingApplyEnd to SignalListener { fail("Change should not be applied") },
                )
            } + mapOf("peer0" to mapOf(Signal.ReachedMaxRetries to SignalListener { phaser.arrive() }))
        )

        val change: Change = change(0, 1)

        expectCatching {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async", change)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
        }
    }

    @Test
    fun `should repeat change if one peerset does not have quorum on change`(): Unit = runBlocking {
        val phaser = Phaser(2)
        // peerset1peer0 - votes abort, peerset1peer1 - votes commit, peerset1peer2 - dies
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
                "peerset1" to listOf("peer3", "peer4", "peer5"),
            ),
            configOverrides = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    "raft.isEnabled" to false,
                )
            } + mapOf("peer3" to mapOf("gpac.abortOnElectMe" to true))
                    + mapOf(
                "peer0" to mapOf(
                    "gpac.initialRetriesDelay" to Duration.ZERO,
                    "gpac.retriesBackoffTimeout" to Duration.ZERO

                )
            ),
            signalListeners = (0..5).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.OnHandlingApplyEnd to SignalListener { fail("Change should not be applied") },)

            } + mapOf("peer0" to mapOf(Signal.ReachedMaxRetries to SignalListener { phaser.arrive() }))
                    + mapOf("peer5" to mapOf(Signal.OnHandlingElectBegin to SignalListener { throw RuntimeException() }))
        )

        val change: Change = change(0, 1)

        expectCatching {
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/async", change)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(apps.getPeerAddresses().values).forEach { changes ->
            expectThat(changes.size).isEqualTo(0)
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
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change1)
            executeChange("http://${apps.getPeer("peer0").address}/v2/change/sync", change2)
        }.isSuccess()

        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
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
        "userName",
        peersets = peersetIds.map {
            ChangePeersetInfo(PeersetId("peerset$it"), InitialHistoryEntry.getId())
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
