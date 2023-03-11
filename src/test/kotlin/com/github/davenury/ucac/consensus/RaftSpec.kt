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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis

@ExtendWith(TestLogExtension::class)
class RaftSpec : IntegrationTestBase() {
    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "raft_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {
        val change1 = createChange(null)
        val change2 = createChange(1, userName = "userName2", parentId = change1.toHistoryEntry(PeersetId("peerset0")).getId())

        val peersWithoutLeader = 4

        val phaser = Phaser(peersWithoutLeader)
        phaser.register()

        val peerLeaderElected = SignalListener {
            expectThat(phaser.phase).isEqualTo(0)
            logger.info("Arrived leader elected ${it.subject.getPeerName()}")
            phaser.arrive()
        }

        val peerApplyChange = SignalListener {
            expectThat(phaser.phase).isContainedIn(listOf(1, 2))
            logger.info("Arrived change ${it.subject.getPeerName()}")
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeAccepted to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getPeerAddresses("peerset0")

        phaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")

        // when: peer1 executed change
        expectCatching {
            executeChange("${apps.getPeer("peer0").address}/v2/change/sync", change1)
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
        expectCatching {
            executeChange("${apps.getPeer("peer1").address}/v2/change/sync", change2)
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
        val peersWithoutLeader = 4
        var iter = 0

        val leaderElectedPhaser = Phaser(peersWithoutLeader)
        leaderElectedPhaser.register()

        val phaser = Phaser(peersWithoutLeader)
        phaser.register()

        val peerLeaderElected = SignalListener {
            if(leaderElectedPhaser.phase == 0) {
                logger.info("Arrived ${it.subject.getPeerName()}")
                leaderElectedPhaser.arrive()
            }
        }

        val peerChangeAccepted = SignalListener {
            logger.info("Arrived change$iter: ${it.change}")
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeAccepted to peerChangeAccepted
                )
            }
        )
        val peerAddresses = apps.getPeerAddresses("peerset0")

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")


        var change = createChange(null)

        val endRange = 1000

        var time = 0L

        repeat(endRange) {
            time += measureTimeMillis {
                expectCatching {
                    executeChange("${apps.getPeer("peer0").address}/v2/change/sync", change)
                }.isSuccess()
            }
            phaser.arriveAndAwaitAdvanceWithTimeout()
            iter+=1
            change = createChange(null, parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
        }
        // when: peer1 executed change

        expectThat(time / endRange).isLessThanOrEqualTo(500L)

        askAllForChanges(peerAddresses.values).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(endRange)
        }
    }

    @Test
    fun `change should be applied without waiting for election`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val phaser = Phaser(peersWithoutLeader)
        phaser.register()

        val peerApplyChange = SignalListener {
            expectThat(phaser.phase).isEqualTo(0)
            logger.info("Arrived ${it.subject.getPeerName()}")
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusFollowerChangeAccepted to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getPeerAddresses("peerset0")

        logger.info("Sending change")

        val change = createChange(null)
        expectCatching {
            executeChange("${apps.getPeer("peer0").address}/v2/change/sync", change)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Change 1 applied")

        askAllForChanges(peerAddresses.values).forEach { changes ->
            expectThat(changes.size).isEqualTo(1)
            expect {
                that(changes[0]).isEqualTo(change)
            }
        }
    }

    @Test
    fun `less than half of peers respond on ConsensusElectMe`(): Unit = runBlocking {
        val activePeers = 2
        val triesToBecomeLeader = 2
        val phaser = Phaser(activePeers * triesToBecomeLeader)

        val peerTryToBecomeLeader = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            phaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
        )
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            appsToExclude = listOf("peer2", "peer3", "peer4"),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()

        apps.getRunningApps().forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
//              DONE  it should always be noneLeader
                that(leaderAddress).isEqualTo(noneLeader)
            }
        }
    }

    @Test
    fun `minimum number of peers respond on ConsensusElectMe`(): Unit = runBlocking {
        val peersWithoutLeader = 3
        val phaser = Phaser(peersWithoutLeader)
        var isLeaderElected = false

        val peerLeaderElected = SignalListener {
            if (!isLeaderElected) {
                logger.info("Arrived ${it.subject.getPeerName()}")
                phaser.arrive()
            } else {
                logger.debug("Leader is elected, not arriving")
            }
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            appsToExclude = listOf("peer3", "peer4"),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()
        isLeaderElected = true

        apps.getRunningApps().forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
                that(leaderAddress).isNotEqualTo(noneLeader)
            }
        }
    }

    @Test
    fun `leader failed and new leader is elected`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val election1Phaser = Phaser(peersWithoutLeader)
        val election2Phaser = Phaser(peersWithoutLeader - 1)
        listOf(election1Phaser, election2Phaser).forEach { it.register() }

        val peerLeaderElected = SignalListener {
            if (election1Phaser.phase == 0) election1Phaser.arrive() else election2Phaser.arrive()
        }

        val signalListener = mapOf(Signal.ConsensusLeaderElected to peerLeaderElected)

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        var peers = apps.getRunningApps()

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

        peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress =
                askForLeaderAddress(peers.first())
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress.address)
        }
    }


    @Test
    fun `less than half peers failed`(): Unit = runBlocking {
        val peersWithoutLeader = 3

        val election1Phaser = Phaser(peersWithoutLeader)
        val election2Phaser = Phaser(peersWithoutLeader - 1)
        listOf(election1Phaser, election2Phaser).forEach { it.register() }

        val peerLeaderElected = SignalListener {
            if (election1Phaser.phase == 0) {
                logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                election1Phaser.arrive()
            } else {
                logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                election2Phaser.arrive()
            }
        }

        val signalListener = mapOf(Signal.ConsensusLeaderElected to peerLeaderElected)

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
            appsToExclude = listOf("peer4"),
        )
        var peers = apps.getRunningApps()

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

        peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress =
                askForLeaderAddress(peers.first())
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress.address)
        }
    }

    //    DONE: Exactly half of peers is running
    @Test
    fun `exactly half of peers is failed`(): Unit = runBlocking {
        val peersWithoutLeader = 3
        val activePeers = 3
        val peersTried: MutableSet<String> = mutableSetOf()
        var leaderElect = false

        val leaderFailedPhaser = Phaser(peersWithoutLeader)
        val electionPhaser = Phaser(peersWithoutLeader)
        val tryToBecomeLeaderPhaser = Phaser(activePeers)

        listOf(leaderFailedPhaser, electionPhaser, tryToBecomeLeaderPhaser).forEach { it.register() }

        val peerTryToBecomeLeader = SignalListener {
            val name = it.subject.getPeerName()
            if (!peersTried.contains(name) && leaderElect) {
                peersTried.add(name)
                logger.info("Arrived peerTryToBecomeLeader ${it.subject.getPeerName()}")
                tryToBecomeLeaderPhaser.arrive()
            }
        }

        val peerLeaderFailed = SignalListener {
            logger.info("Arrived peerLeaderFailed ${it.subject.getPeerName()}")
            leaderFailedPhaser.arrive()
        }
        val peerLeaderElected = SignalListener {
            logger.info("Arrived peerLeaderElected ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderDoesNotSendHeartbeat to peerLeaderFailed,
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4", "peer5"),
            ),
            appsToExclude = listOf("peer4", "peer5"),
            signalListeners = (0..5).map { "peer$it" }.associateWith { signalListener },
        )
        var peers = apps.getRunningApps()

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        apps.getApp(firstLeaderAddress.peerId).stop(0, 0)
        leaderElect = true

        peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

        leaderFailedPhaser.arriveAndAwaitAdvanceWithTimeout()
        tryToBecomeLeaderPhaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress = askForLeaderAddress(peers.first())
            that(secondLeaderAddress).isEqualTo(noneLeader)
        }
    }

    @Test
    fun `leader fails during processing change`(): Unit = runBlocking {
        val change = createChange(null)
        var peersWithoutLeader = 4

        val failurePhaser = Phaser(2)
        val election1Phaser = Phaser(peersWithoutLeader)
        peersWithoutLeader -= 1
        val election2Phaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(3)
        listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
        var firstLeader = true
        val proposedPeers = ConcurrentHashMap<String, Boolean>()
        var changePeers: (() -> Unit?)? = null

        val mutex = Mutex()

        val leaderAction = SignalListener {
            if (firstLeader) {
                logger.info("Arrived ${it.subject.getPeerName()}")
                changePeers?.invoke()
                failurePhaser.arrive()
                throw RuntimeException("Failed after proposing change")
            }
        }

        val peerLeaderElected =
            SignalListener {
                when (election1Phaser.phase) {
                    0 -> {
                        logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                        election1Phaser.arrive()
                    }

                    else -> {
                        logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                        firstLeader = false
                        election2Phaser.arrive()
                    }
                }
            }

        val peerApplyChange = SignalListener {
            logger.info("Arrived peer apply change")
            changePhaser.arrive()
        }
        val ignoreHeartbeatAfterProposingChange = SignalListener {
            runBlocking {
                mutex.withLock {
                    when {
                        it.change == change && firstLeader && !proposedPeers.contains(it.subject.getPeerName()) -> {
                            proposedPeers[it.subject.getPeerName()] = true
                        }

                        proposedPeers.contains(it.subject.getPeerName()) && firstLeader -> throw Exception("Ignore heartbeat from old leader")
                        proposedPeers.size > 2 && firstLeader -> throw Exception("Ignore heartbeat from old leader")
                    }
                }
            }
        }

        val signalListener = mapOf(
            Signal.ConsensusAfterProposingChange to leaderAction,
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeAccepted to peerApplyChange,
            Signal.ConsensusFollowerHeartbeatReceived to ignoreHeartbeatAfterProposingChange
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(apps.getRunningApps()[0])

        changePeers = {
            val peers = apps.getRunningPeers("peerset0").mapValues { entry ->
                val peer = entry.value
                peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
            }
            apps.getApp(firstLeaderAddress.peerId).setPeerAddresses(peers)
        }


//      Start processing
        expectCatching {
            executeChange("${firstLeaderAddress.address}/v2/change/sync?timeout=PT0.5S", change)
        }.isFailure()

        failurePhaser.arriveAndAwaitAdvanceWithTimeout()

        apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()
        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers("peerset0")
            .values
            .filter { it != firstLeaderAddress }
            .forEach {
                val proposedChanges2 = askForProposedChanges(it)
                val acceptedChanges2 = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges2.size).isEqualTo(0)
                    that(acceptedChanges2.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges2.first()).isEqualTo(change)
                    that(acceptedChanges2.first().acceptNum).isEqualTo(null)
                }
            }

    }

    @Test
    fun `less than half of peers fails after electing leader`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val electionPhaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader - 2)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to SignalListener {
                logger.info("Arrived at election ${it.subject.getPeerName()}")
                electionPhaser.arrive()
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                logger.info("Arrived at apply ${it.subject.getPeerName()}")
                changePhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers("peerset0").values

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(2)
        peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
        val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
        val change = createChange(null)

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first().address}/v2/change/sync", change)
        }.isSuccess()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()

        runningPeers.forEach {
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
    fun `more than half of peers fails during propagating change`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val electionPhaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader - 3)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener { electionPhaser.arrive() }
        val peerApplyChange = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            changePhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeProposed to peerApplyChange,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers("peerset0").values

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(3)
        peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
        val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
        val change = createChange(null)

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first().address}/v2/change/async", change)
        }.isSuccess()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()

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
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers("peerset0").values
        val peerAddresses2 = apps.getRunningPeers("peerset0")

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
            val peers = apps.getRunningPeers("peerset0").mapValues { entry ->
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
            val peers = apps.getRunningPeers("peerset0").mapValues { entry ->
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

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("Second election finished")

//      Check if second half chose new leader
        secondHalf.forEachIndexed { index, peer ->
            val app = apps.getApp(peer.peerId)
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
            apps.getApp(address.peerId).setPeerAddresses(peerAddresses2)
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
        val consensus = RaftConsensusProtocolImpl(
            PeersetId("peerset0"),
            InMemoryHistory(),
            Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
            peerResolver,
            protocolClient = RaftProtocolClientImpl(),
            transactionBlocker = TransactionBlocker(),
            isMetricTest = false,
            maxChangesPerMessage = 200
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
    fun `should synchronize on history if it was added outside of raft`(): Unit = runBlocking {
        val phaserGPACPeer = Phaser(1)
        val phaserRaftPeers = Phaser(4)
        val leaderElectedPhaser = Phaser(4)

        val isSecondGPAC = AtomicBoolean(false)

        listOf(phaserGPACPeer, phaserRaftPeers, leaderElectedPhaser).forEach { it.register() }

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
                Signal.ConsensusLeaderElected to leaderElectedAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
            )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = mapOf(
                "peer0" to firstPeerSignals,
                "peer1" to peer1Signals,
                "peer2" to peerSignals,
                "peer3" to peerRaftSignals,
                "peer4" to peerRaftSignals,
            ),
            configOverrides = mapOf(
                "peer0" to mapOf("gpac.maxLeaderElectionTries" to 2),
                "peer1" to mapOf("gpac.maxLeaderElectionTries" to 2),
                "peer2" to mapOf("gpac.maxLeaderElectionTries" to 2),
                "peer3" to mapOf("gpac.maxLeaderElectionTries" to 2),
                "peer4" to mapOf("gpac.maxLeaderElectionTries" to 2),
            ),
        )

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // change that will cause leader to fall according to action
        try {
            executeChange(
                "${apps.getPeer("peer0").address}/v2/change/sync?enforce_gpac=true",
                proposedChange
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
        isSecondGPAC.set(true)

        val change = testHttpClient.get<Change>("http://${apps.getPeer("peer1").address}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(proposedChange.groupName)
        }

        phaserRaftPeers.arriveAndAwaitAdvanceWithTimeout()

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

    @Test
    fun `one peer consensus`(): Unit = runBlocking {
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0"),
            ),
        )

        expectCatching {
            val change1 = createChange(null)
            val peerAddress = apps.getPeer("peer0").address
            val change2 = createChange(null, parentId = change1.toHistoryEntry(PeersetId("peerset0")).getId())
            executeChange("$peerAddress/v2/change/sync", change1)
            executeChange("$peerAddress/v2/change/sync", change2)
        }.isSuccess()

        askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
            expectThat(changes.size).isEqualTo(2)
        }
    }

    private fun createChange(
        acceptNum: Int?,
        userName: String = "userName",
        parentId: String = InitialHistoryEntry.getId(),
    ) = AddUserChange(
        userName,
        acceptNum,
        peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), parentId)),
    )

    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>("http://${uri}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun genericAskForChange(suffix: String, peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/raft/$suffix") {
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

    private fun askForLeaderAddress(app: ApplicationUcac): String? {
        val leaderId = app.getConsensusProtocol().getLeaderId()
        return leaderId?.let { apps.getPeer(it).address }
    }

    private fun getLeaderAddress(app: ApplicationUcac): PeerAddress {
        val leaderId = app.getConsensusProtocol().getLeaderId()!!
        return apps.getPeer(leaderId)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftSpec::class.java)
    }
}
