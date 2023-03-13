package com.github.davenury.ucac.consensus

import com.github.davenury.common.*
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
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
import org.junit.jupiter.api.Disabled
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

//@Disabled
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
            listOf(5),
            signalListeners = (0..4).associateWith {
                mapOf(
                    Signal.AlvinCommitChange to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getPeers(0)

        delay(100)

        // when: peer1 executed change
        val change1 = createChange(null)
        expectCatching {
            executeChange("${apps.getPeer(0, 0).address}/v2/change/sync", change1)
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
        val change2 = createChange(1, userName = "userName2", parentId = change1.toHistoryEntry(0).getId())
        expectCatching {
            executeChange("${apps.getPeer(0, 1).address}/v2/change/sync", change2)
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
            listOf(5),
            signalListeners = (0..4).associateWith {
                if(it == 0) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(0, 0).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(0)
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
            listOf(5),
            signalListeners = (0..4).associateWith {
                if(it == 0) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(0, 0).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(0)
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
            listOf(5),
            signalListeners = (0..4).associateWith {
                if(it == 0) firstLeaderListener
                else signalListener
            },
        )

//      Start processing
        expectCatching {
            executeChange("${apps.getPeer(0, 0).address}/v2/change/sync?timeout=PT4S", change)
        }.isFailure()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(0)
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
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )

        val peerAddresses = apps.getRunningPeers(0).values


        val peersToStop = peerAddresses.take(3)
        peersToStop.forEach { apps.getApp(it.globalPeerId).stop(0, 0) }
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

    @Disabled
    @Test
    fun `network divide on half and then merge`(): Unit = runBlocking {
        var peersWithoutLeader = 4

        var isNetworkDivided = false

        val election1Phaser = Phaser(peersWithoutLeader)
        peersWithoutLeader -= 2
        val election2Phaser = Phaser(peersWithoutLeader)
        val change1Phaser = Phaser(peersWithoutLeader)
        val change2Phaser = Phaser(peersWithoutLeader)
        listOf(election1Phaser, election2Phaser, change1Phaser, change2Phaser).forEach { it.register() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to SignalListener {
                when {
                    election1Phaser.phase == 0 -> {
                        logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                        election1Phaser.arrive()
                    }

                    isNetworkDivided && election2Phaser.phase == 0 -> {
                        logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                        election2Phaser.arrive()
                    }
                }
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (change1Phaser.phase == 0) {
                    logger.info("Arrived at change 1 ${it.subject.getPeerName()}")
                    change1Phaser.arrive()
                } else {
                    logger.info("Arrived at change 2 ${it.subject.getPeerName()}")
                    change2Phaser.arrive()
                }
            }
        )

        apps = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers(0).values
        val peerAddresses2 = apps.getRunningPeers(0)

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("First election finished")

        val firstLeaderAddress = getLeaderAddress(peers[0])

        logger.info("First leader: $firstLeaderAddress")

        val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

        val firstHalf: List<PeerAddress> = listOf(firstLeaderAddress, notLeaderPeers.first())
        val secondHalf: List<PeerAddress> = notLeaderPeers.drop(1)

//      Divide network
        isNetworkDivided = true

        firstHalf.forEach { address ->
            val peers = apps.getRunningPeers(0).mapValues { entry ->
                val peer = entry.value
                if (secondHalf.contains(peer)) {
                    peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                } else {
                    peer
                }
            }
            apps.getApp(address.globalPeerId).setPeers(peers)
        }

        secondHalf.forEach { address ->
            val peers = apps.getRunningPeers(0).mapValues { entry ->
                val peer = entry.value
                if (firstHalf.contains(peer)) {
                    peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                } else {
                    peer
                }
            }
            apps.getApp(address.globalPeerId).setPeers(peers)
        }

        logger.info("Network divided")

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("Second election finished")

//      Check if second half chose new leader
        secondHalf.forEachIndexed { index, peer ->
            val app = apps.getApp(peer.globalPeerId)
            val newLeaderAddress = askForLeaderAddress(app)
            expectThat(newLeaderAddress).isNotEqualTo(firstLeaderAddress.address)

            // log only once
            if (index == 0) {
                logger.info("New leader: $newLeaderAddress")
            }
        }

        val change1 = createChange(1)
        val change2 = createChange(2)

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first().address}/v2/change/async", change1)
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first().address}/v2/change/async", change2)
        }.isSuccess()

        change1Phaser.arriveAndAwaitAdvanceWithTimeout()

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
            apps.getApp(address.globalPeerId).setPeers(peerAddresses2)
        }

        logger.info("Network merged")

        change2Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 2")

        peerAddresses.forEach {
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
    }

    @Test
    fun `unit tests of isMoreThanHalf() function`(): Unit = runBlocking {
        val peers = listOf(
            PeerAddress(GlobalPeerId(0, 0), "1"),
            PeerAddress(GlobalPeerId(0, 1), "2"),
            PeerAddress(GlobalPeerId(0, 2), "3"),
        )
            .associateBy { it.globalPeerId }
            .toMutableMap()

        val peerResolver = PeerResolver(GlobalPeerId(0, 0), peers)
        val consensus = RaftConsensusProtocolImpl(
            InMemoryHistory(),
            "1",
            Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
            peerResolver,
            protocolClient = RaftProtocolClientImpl(),
            transactionBlocker = TransactionBlocker(),
            isMetricTest = false
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isTrue()
            that(consensus.isMoreThanHalf(2)).isTrue()
        }

        peers[GlobalPeerId(0, 3)] = PeerAddress(GlobalPeerId(0, 3), "4")
        peerResolver.setPeers(peers)
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
        }

        peers[GlobalPeerId(0, 4)] = PeerAddress(GlobalPeerId(0, 4), "5")
        peerResolver.setPeers(peers)
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
        }

        peers[GlobalPeerId(0, 5)] = PeerAddress(GlobalPeerId(0, 5), "6")
        peerResolver.setPeers(peers)
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isFalse()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
            that(consensus.isMoreThanHalf(5)).isTrue()
        }
    }

    @Disabled
    @Test
    fun `should synchronize on history if it was added outside of alvin`(): Unit = runBlocking {
        val phaserGPACPeer = Phaser(1)
        val phaserRaftPeers = Phaser(4)
        val leaderElectedPhaser = Phaser(4)

        val isSecondGPAC = AtomicBoolean(false)

        listOf(phaserGPACPeer, phaserRaftPeers, leaderElectedPhaser).forEach { it.register() }

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


        val peerGPACAction = SignalListener {
            phaserGPACPeer.arrive()
        }
        val raftPeersAction = SignalListener {
            phaserRaftPeers.arrive()
        }
        val leaderElectedAction = SignalListener { leaderElectedPhaser.arrive() }

        val firstPeerSignals = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction,
            Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
            Signal.ConsensusLeaderElected to leaderElectedAction,
            Signal.OnHandlingElectBegin to SignalListener {
                if (isSecondGPAC.get()) {
                    throw Exception("Ignore restarting GPAC")
                }
            }
        )

        val peerSignals =
            mapOf(
                Signal.ConsensusLeaderElected to leaderElectedAction,
                Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") }
            )

        val peerRaftSignals =
            mapOf(
                Signal.ConsensusLeaderElected to leaderElectedAction,
                Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                Signal.OnHandlingAgreeBegin to SignalListener { throw Exception("Ignore GPAC") }
            )

        val peer1Signals =
            mapOf(
                Signal.OnHandlingApplyCommitted to peerGPACAction,
                Signal.ConsensusLeaderElected to leaderElectedAction
            )

        apps = TestApplicationSet(
            listOf(5),
            signalListeners = mapOf(
                0 to firstPeerSignals,
                1 to peer1Signals,
                2 to peerSignals,
                3 to peerRaftSignals,
                4 to peerRaftSignals,
            ), configOverrides = mapOf(
                0 to mapOf("gpac.maxLeaderElectionTries" to 2),
                1 to mapOf("gpac.maxLeaderElectionTries" to 2),
                2 to mapOf("gpac.maxLeaderElectionTries" to 2),
                3 to mapOf("gpac.maxLeaderElectionTries" to 2),
                4 to mapOf("gpac.maxLeaderElectionTries" to 2),
            )
        )

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // change that will cause leader to fall according to action
        try {
            executeChange(
                "${apps.getPeer(0, 0).address}/v2/change/sync?enforce_gpac=true",
                proposedChange
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
        isSecondGPAC.set(true)

        val change = testHttpClient.get<Change>("http://${apps.getPeer(0, 1).address}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(proposedChange.groupName)
        }

        phaserRaftPeers.arriveAndAwaitAdvanceWithTimeout()

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

    private fun createChange(
        acceptNum: Int?,
        userName: String = "userName",
        parentId: String = InitialHistoryEntry.getId(),
    ) = AddUserChange(
        userName,
        acceptNum,
        peersets = listOf(ChangePeersetInfo(0, parentId)),
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

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { askForChanges(it) }

    private suspend fun askForProposedChanges(peerAddress: PeerAddress) =
        genericAskForChange("proposed_changes", peerAddress)

    private suspend fun askForAcceptedChanges(peerAddress: PeerAddress) =
        genericAskForChange("accepted_changes", peerAddress)

    private suspend fun askForLeaderAddress(app: ApplicationUcac): String? {
        val consensusProperty =
            ApplicationUcac::class.declaredMemberProperties.single { it.name == "consensusProtocol" }
        val consensusOldAccessible = consensusProperty.isAccessible
        try {
            consensusProperty.isAccessible = true
            val consensusProtocol = consensusProperty.get(app) as RaftConsensusProtocolImpl
            return consensusProtocol.getLeaderAddress()
        } finally {
            consensusProperty.isAccessible = consensusOldAccessible
        }
    }

    private suspend fun getLeaderAddress(peer: ApplicationUcac): PeerAddress {
        val address = askForLeaderAddress(peer)!!
        expectThat(address).isNotEqualTo(noneLeader)
        val id = apps.getPeers().values.find { it.address == address }!!.globalPeerId
        return PeerAddress(id, address)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AlvinSpec::class.java)
    }
}
