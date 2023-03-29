package com.github.davenury.ucac.consensus

import com.github.davenury.common.*
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import com.github.davenury.ucac.consensus.alvin.AlvinProtocol
import com.github.davenury.ucac.consensus.alvin.AlvinProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.util.collections.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.*
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.system.measureTimeMillis

@ExtendWith(TestLogExtension::class)
class AlvinSpec : IntegrationTestBase() {

    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "alvin_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {
        val peers = 5

        val phaser = Phaser(peers)
        phaser.register()


        val peerApplyChange = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.AlvinCommitChange to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getRunningPeers("peerset0")

        delay(100)

        // when: peer1 executed change
        val change1 = createChange(null)
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync", change1)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Change 1 applied")

        askAllForChanges(peerAddresses.values).forEach { changes ->
            // then: there's one change, and it's change we've requested
            expectThat(changes.size).isEqualTo(1)
            expect {
                that(changes[0]).isEqualTo(change1)
                that(changes[0].acceptNum).isEqualTo(null)
            }
        }

        // when: peer2 executes change
        val change2 = createChange(1, userName = "userName2", parentId = change1.toHistoryEntry(peerset(0)).getId())
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync", change2)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Change 2 applied")

        askAllForChanges(peerAddresses.values).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(2)
            expect {
                that(changes[1]).isEqualTo(change2)
                that(changes[0]).isEqualTo(change1)
                that(changes[1].acceptNum).isEqualTo(1)
            }
        }
    }

    @Test
    fun `1000 change processed sequentially`(): Unit = runBlocking {
        val peers = 5

        val phaser = Phaser(peers)
        phaser.register()


        val peerChangeAccepted = SignalListener {
            logger.info("Arrived change: ${it.change}")
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith {
                mapOf(
                    Signal.AlvinCommitChange to peerChangeAccepted
                )
            }
        )
        val peerAddresses = apps.getRunningPeers("peerset0")

        var change = createChange(null)

        val endRange = 1000

        var time = 0L

        (0 until endRange).forEach {
            val newTime = measureTimeMillis {
                expectCatching {
                    executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync", change)
                }.isSuccess()
            }
            logger.info("Change $it is processed $newTime ms")
            time += newTime
            phaser.arriveAndAwaitAdvanceWithTimeout()
            change = createChange(null, parentId = change.toHistoryEntry(peerset(0)).getId())
        }
        // when: peer1 executed change

        expectThat(time / endRange).isLessThanOrEqualTo(500L)

        askAllForChanges(peerAddresses.values).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(endRange)
        }
    }

    @Test
    fun `change leader fails after proposal`(): Unit = runBlocking {
        val change = createChange(null)
        var allPeers = 5

        val changePhaser = Phaser(allPeers)
        changePhaser.register()

        val peerApplyChange = SignalListener {
            logger.info("Arrived peer apply change")
            changePhaser.arrive()
        }

        val afterProposalPhase = SignalListener {
            throw RuntimeException("Test failure after proposal")
        }

        val signalListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
        )

        val firstLeaderListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
            Signal.AlvinAfterProposalPhase to afterProposalPhase,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith {
                if (it == peer(0)) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(peerset(0).peersetId)
            .values
            .forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change)
                    that(acceptedChanges.first().acceptNum).isEqualTo(null)
                }
            }

    }


    @Test
    fun `change leader fails after accept`(): Unit = runBlocking {
        val change = createChange(null)
        var allPeers = 5

        val changePhaser = Phaser(allPeers)
        changePhaser.register()

        val peerApplyChange = SignalListener {
            logger.info("Arrived peer apply change")
            changePhaser.arrive()
        }

        val afterAcceptPhase = SignalListener {
            throw RuntimeException("Test failure after accept")
        }

        val signalListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
        )

        val firstLeaderListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
            Signal.AlvinAfterAcceptPhase to afterAcceptPhase,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith {
                if (it == peer(0)) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(peerset(0).peersetId)
            .values
            .forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change)
                    that(acceptedChanges.first().acceptNum).isEqualTo(null)
                }
            }
    }

    @Test
    fun `change leader fails after stable`(): Unit = runBlocking {
        val change = createChange(null)
        var allPeers = 5

        val changePhaser = Phaser(allPeers)
        changePhaser.register()

        val peerApplyChange = SignalListener {
            logger.info("Arrived peer apply change")
            changePhaser.arrive()
        }

        val afterStablePhase = SignalListener {
            throw RuntimeException("Test failure after stable")
        }

        val signalListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
        )

        val firstLeaderListener = mapOf(
            Signal.AlvinCommitChange to peerApplyChange,
            Signal.AlvinAfterStablePhase to afterStablePhase,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith {
                if (it == peer(0)) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(peerset(0).peersetId)
            .values
            .forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change)
                    that(acceptedChanges.first().acceptNum).isEqualTo(null)
                }
            }
    }


    @Test
    fun `more than half of peers fails before propagating change`(): Unit = runBlocking {
        val changeProposedPhaser = Phaser(2)

        val peerReceiveProposal = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            changeProposedPhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.AlvinReceiveProposal to peerReceiveProposal,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith { signalListener },
        )

        val peerAddresses = apps.getRunningPeers(peerset(0).peersetId).values


        val peersToStop = peerAddresses.take(3)
        peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
        val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
        val change = createChange(null)

        delay(500)

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first().address}/v2/change/async", change)
        }.isSuccess()

        changeProposedPhaser.arriveAndAwaitAdvanceWithTimeout()

//      As only one peer confirm changes it should be still proposedChange
        runningPeers.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            expect {
                that(proposedChanges.size).isEqualTo(1)
                that(acceptedChanges.size).isEqualTo(0)
            }
            expect {
                that(proposedChanges.first()).isEqualTo(change)
                that(proposedChanges.first().acceptNum).isEqualTo(null)
            }
        }
    }

    @Test
    fun `network divide on half and then merge`(): Unit = runBlocking {
        val change1AbortPhaser = Phaser(5)
        val change2PropagatePhaser = Phaser(2)
        val change2CommitPhaser = Phaser(3)
        val change1 = createChange(1)
        val change2 = createChange(2)


        listOf(change1AbortPhaser, change2CommitPhaser, change2PropagatePhaser).forEach { it.register() }

        val signalListener = mapOf(
            Signal.AlvinCommitChange to SignalListener {
                if (change2CommitPhaser.phase == 0) {
                    logger.info("Arrived change before committing ${it.subject.getPeerName()}")
                    change2CommitPhaser.arrive()
                } else {
                    logger.info("Arrived change after committing ${it.subject.getPeerName()}")
                    change2PropagatePhaser.arrive()
                }
            },
            Signal.AlvinAbortChange to SignalListener {
                logger.info("Arrived change abort ${it.subject.getPeerName()}")
                change1AbortPhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith { signalListener },
        )

        val peerAddresses = apps.getRunningPeers(peerset(0).peersetId).values
        val peerAddresses2 = apps.getRunningPeers(peerset(0).peersetId)

        val firstHalf: List<PeerAddress> = peerAddresses.take(2)
        val secondHalf: List<PeerAddress> = peerAddresses.drop(2)

//      Divide network

        firstHalf.forEach { address ->
            val peers = apps.getRunningPeers(peerset(0).peersetId).mapValues { entry ->
                val peer = entry.value
                if (secondHalf.contains(peer)) {
                    peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                } else {
                    peer
                }
            }
            apps.getApp(address.peerId).setPeerAddresses(peers)
        }

        secondHalf.forEach { address ->
            val peers = apps.getRunningPeers(peerset(0).peersetId).mapValues { entry ->
                val peer = entry.value
                if (firstHalf.contains(peer)) {
                    peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                } else {
                    peer
                }
            }
            apps.getApp(address.peerId).setPeerAddresses(peers)
        }

        logger.info("Network divided")

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first().address}/v2/change/async", change1)
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first().address}/v2/change/async", change2)
        }.isSuccess()

        change2CommitPhaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 1")

        firstHalf.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
            expect {
                that(proposedChanges.size).isEqualTo(1)
                that(acceptedChanges.size).isEqualTo(0)
            }
            expect {
                that(proposedChanges.first()).isEqualTo(change1)
                that(proposedChanges.first().acceptNum).isEqualTo(1)
            }
        }

        secondHalf.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
            expect {
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
            }
            expect {
                that(acceptedChanges.first()).isEqualTo(change2)
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }

//      Merge network
        peerAddresses.forEach { address ->
            apps.getApp(address.peerId).setPeerAddresses(peerAddresses2)
        }

        logger.info("Network merged")

        change2PropagatePhaser.arriveAndAwaitAdvanceWithTimeout()
        change1AbortPhaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 2")

        peerAddresses.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
            expect {
                that(it).isEqualTo(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
            }
            expect {
                that(acceptedChanges.first()).isEqualTo(change2)
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }
    }

    @Test
    fun `unit tests of isMoreThanHalf() function`(): Unit = runBlocking {
        val peers = listOf(
            PeerAddress(PeerId("peer0"), "1"),
            PeerAddress(PeerId("peer1"), "2"),
            PeerAddress(PeerId("peer2"), "3"),
            PeerAddress(PeerId("peer3"), "4"),
            PeerAddress(PeerId("peer4"), "5"),
            PeerAddress(PeerId("peer5"), "6"),
        )
            .associateBy { it.peerId }
            .toMutableMap()

        val peerResolver = PeerResolver(
            PeerId("peer0"), peers, mapOf(
                PeersetId("peerset0") to listOf(PeerId("peer0"), PeerId("peer1"), PeerId("peer2"))
            )
        )
        val consensus = AlvinProtocol(
            PeersetId("peerset0"),
            InMemoryHistory(),
            Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
            peerResolver,
            protocolClient = AlvinProtocolClientImpl(),
            transactionBlocker = TransactionBlocker(),
            isMetricTest = false
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isTrue()
            that(consensus.isMoreThanHalf(2)).isTrue()
        }

        peerResolver.addPeerToPeerset(
            PeersetId("peerset0"),
            PeerId("peer3"),
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
        }

        peerResolver.addPeerToPeerset(
            PeersetId("peerset0"),
            PeerId("peer4"),
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
        }

        peerResolver.addPeerToPeerset(
            PeersetId("peerset0"),
            PeerId("peer4"),
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isFalse()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
            that(consensus.isMoreThanHalf(5)).isTrue()
        }
    }

    @Test
    fun `should synchronize on history if it was added outside of alvin`(): Unit = runBlocking {
        val phaserGPACPeer = Phaser(1)
        val phaserAlvinPeers = Phaser(5)

        val isSecondGPAC = AtomicBoolean(false)

        listOf(phaserGPACPeer, phaserAlvinPeers).forEach { it.register() }

        val change1 = AddGroupChange(
            "name",
            peersets = listOf(
                ChangePeersetInfo(
                    peerset(0),
                    InitialHistoryEntry.getId()
                )
            )
        )
        val change2 = AddGroupChange(
            "name",
            peersets = listOf(
                ChangePeersetInfo(
                    peerset(0),
                    change1.toHistoryEntry(peerset(0)).getId()
                )
            )
        )

        val firstLeaderAction = SignalListener { signalData ->
            val url = "http://${signalData.peerResolver.resolve(peerId(1)).address}/apply"
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


        val peerGPACAction = SignalListener {
            phaserGPACPeer.arrive()
        }
        val consensusPeersAction = SignalListener {
            logger.info("Arrived: ${it.change}")
            if (it.change == change2) phaserAlvinPeers.arrive()
        }

        val firstPeerSignals = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction,
            Signal.AlvinCommitChange to consensusPeersAction,
            Signal.OnHandlingElectBegin to SignalListener {
                if (isSecondGPAC.get()) {
                    throw Exception("Ignore restarting GPAC")
                }
            }
        )

        val peerSignals =
            mapOf(
                Signal.AlvinCommitChange to consensusPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") }
            )

        val peerRaftSignals =
            mapOf(
                Signal.AlvinCommitChange to consensusPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                Signal.OnHandlingAgreeBegin to SignalListener { throw Exception("Ignore GPAC") }
            )

        val peer1Signals =
            mapOf(
                Signal.AlvinCommitChange to consensusPeersAction,
                Signal.OnHandlingApplyCommitted to peerGPACAction,
            )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = mapOf(
                peer(0) to firstPeerSignals,
                peer(1) to peer1Signals,
                peer(2) to peerSignals,
                peer(3) to peerRaftSignals,
                peer(4) to peerRaftSignals,
            ), configOverrides = mapOf(
                peer(0) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(1) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(2) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(3) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(4) to mapOf("gpac.maxLeaderElectionTries" to 2),
            )
        )


        // change that will cause leader to fall according to action
        try {
            executeChange(
                "${apps.getPeer(peer(0)).address}/v2/change/sync?enforce_gpac=true",
                change1
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
        isSecondGPAC.set(true)

        val change = testHttpClient.get<Change>("http://${apps.getPeer(peer(1)).address}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(change1.groupName)
        }

        executeChange(
            "${apps.getPeer(peer(1)).address}/v2/change/sync",
            change2
        )

        phaserAlvinPeers.arriveAndAwaitAdvanceWithTimeout()

        apps.getPeerAddresses(peerset(0)).forEach { (_, peerAddress) ->
            // and should not execute this change couple of times
            val changes = testHttpClient.get<Changes>("http://${peerAddress.address}/changes") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            expectThat(changes.size).isGreaterThanOrEqualTo(2)
            expect {
                that(changes[0]).isA<AddGroupChange>()
                that((changes[0] as AddGroupChange).groupName).isEqualTo(change1.groupName)
            }
            expect {
                that(changes[1]).isA<AddGroupChange>()
                that((changes[1] as AddGroupChange).groupName).isEqualTo(change2.groupName)
            }
        }
    }

    private fun createChange(
        acceptNum: Int?,
        userName: String = "userName",
        parentId: String = InitialHistoryEntry.getId(),
    ) = AddUserChange(
        userName,
        acceptNum,
        peersets = listOf(ChangePeersetInfo(peerset(0), parentId)),
    )

    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>("http://${uri}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun genericAskForChange(suffix: String, peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/alvin/$suffix") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }


    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/v2/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private fun peer(peerId: Int): String = "peer$peerId"
    private fun peerId(peerId: Int): PeerId = PeerId(peer(peerId))
    private fun peerset(peersetId: Int): PeersetId = PeersetId("peerset$peersetId")

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { askForChanges(it) }

    private suspend fun askForProposedChanges(peerAddress: PeerAddress) =
        genericAskForChange("proposed_changes", peerAddress)

    private suspend fun askForAcceptedChanges(peerAddress: PeerAddress) =
        genericAskForChange("accepted_changes", peerAddress)

    companion object {
        private val logger = LoggerFactory.getLogger(AlvinSpec::class.java)
    }
}
