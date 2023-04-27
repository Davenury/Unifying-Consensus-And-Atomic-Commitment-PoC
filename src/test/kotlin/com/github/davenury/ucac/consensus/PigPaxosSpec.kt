package com.github.davenury.ucac.consensus

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.consensus.pigpaxos.PigPaxosProtocol
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
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis

@ExtendWith(TestLogExtension::class)
class PigPaxosSpec : IntegrationTestBase() {

    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "pigpaxos_application.conf")
    }


    @Test
    fun `happy path`(): Unit = runBlocking {
        val peersWithoutLeader = 5

        val leaderElectionPhaser = Phaser(1)
        val changePhaser = Phaser(peersWithoutLeader)
        listOf(leaderElectionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener {
            logger.info("Arrived leader election ${it.subject.getPeerName()}")
            leaderElectionPhaser.arrive()
        }

        val peerApplyChange = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            changePhaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { peer(it) }.associateWith {
                mapOf(
                    Signal.PigPaxosLeaderElected to peerLeaderElected,
                    Signal.PigPaxosChangeCommitted to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getRunningPeers("peerset0")

        leaderElectionPhaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")

        // when: peer1 executed change
        val change1 = createChange(null)
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?peerset=peerset0", change1)
        }.isSuccess()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()
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
            executeChange("${apps.getPeer(peer(1)).address}/v2/change/sync?peerset=peerset0", change2)
        }.isSuccess()

        changePhaser.arriveAndAwaitAdvanceWithTimeout()
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

        val leaderElectedPhaser = Phaser(1)
        leaderElectedPhaser.register()

        val phaser = Phaser(peers)
        phaser.register()

        var change = createChange(null)

        val peerLeaderElected = SignalListener {
            logger.info("Arrived peer elected ${it.subject.getPeerName()}")
            leaderElectedPhaser.arrive()
        }

        val peerChangeAccepted = SignalListener {
            logger.info("Arrived change: ${it.change}")
            if (it.change == change) phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.PigPaxosLeaderElected to peerLeaderElected,
                    Signal.PigPaxosChangeCommitted to peerChangeAccepted
                )
            }
        )
        val peerAddresses = apps.getRunningPeers("peerset0")

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
        logger.info("Leader elected")


        val endRange = 1000

        var time = 0L

        (0 until endRange).forEach {
            val newTime = measureTimeMillis {
                expectCatching {
                    executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?peerset=peerset0", change)
                }.isSuccess()
            }
            logger.info("Change $it is processed $newTime ms")
            time += newTime
            phaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))
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
    fun `change should be applied without waiting for election`(): Unit = runBlocking {
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
                    Signal.PigPaxosChangeCommitted to peerApplyChange
                )
            }
        )
        val peerAddresses = apps.getRunningPeers("peerset0")

        logger.info("Sending change")

        val change = createChange(null)
        expectCatching {
            executeChange("${apps.getPeer(peer(0)).address}/v2/change/sync?peerset=peerset0", change)
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
            Signal.PigPaxosTryToBecomeLeader to peerTryToBecomeLeader,
        )
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            appsToExclude = listOf("peer2", "peer3", "peer4"),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()

        val leaderNotElected = apps.getRunningApps().any {
            askForLeaderAddress(it) == noneLeader
//              DONE  it should always be noneLeader
        }

        expectThat(leaderNotElected).isTrue()
    }

    @Test
    fun `minimum number of peers respond on ConsensusElectMe`(): Unit = runBlocking {
        val peersWithoutLeader = 2
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
            Signal.PigPaxosLeaderElected to peerLeaderElected,
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
    fun `leader fails during processing change`(): Unit = runBlocking {
        val change = createChange(null)
        var peers = 5

        val failurePhaser = Phaser(2)
        val election1Phaser = Phaser(1)
        val election2Phaser = Phaser(1)
        peers -= 1
        val changePhaser = Phaser(4)
        listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
        var firstLeader = true
        val proposedPeers = ConcurrentHashMap<String, Boolean>()
        var changePeers: (() -> Unit?)? = null

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
                when (failurePhaser.phase) {
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
            when {
                it.change == change && firstLeader && !proposedPeers.contains(it.subject.getPeerName()) -> {
                    proposedPeers[it.subject.getPeerName()] = true
                }

                proposedPeers.contains(it.subject.getPeerName()) && firstLeader -> throw Exception("Ignore heartbeat from old leader")
                proposedPeers.size > 2 && firstLeader -> throw Exception("Ignore heartbeat from old leader")

            }
        }

        val signalListener = mapOf(
            Signal.PigPaxosAfterAcceptChange to leaderAction,
            Signal.PigPaxosLeaderElected to peerLeaderElected,
            Signal.PigPaxosChangeCommitted to peerApplyChange,
            Signal.PigPaxosReceivedCommit to ignoreHeartbeatAfterProposingChange
        )

        apps = TestApplicationSet(
            mapOf("peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4")),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )


        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress =
            apps.getRunningApps().firstNotNullOf { getLeaderAddress(it) }

        changePeers = {
            val peers = apps.getRunningPeers(peerset(0).peersetId).mapValues { entry ->
                val peer = entry.value
                peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
            }
            apps.getApp(firstLeaderAddress.peerId).setPeerAddresses(peers)
        }


//      Start processing
        expectCatching {
            executeChange("${firstLeaderAddress.address}/v2/change/sync?peerset=peerset0&timeout=PT0.5S", change)
        }.isFailure()

        failurePhaser.arriveAndAwaitAdvanceWithTimeout()

        apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()
        changePhaser.arriveAndAwaitAdvanceWithTimeout()


        apps.getRunningPeers(peerset(0).peersetId)
            .values
            .filter { it != firstLeaderAddress }
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
    fun `less than half of peers fails after electing leader`(): Unit = runBlocking {
        val allPeers = 5

        val electionPhaser = Phaser(1)
        val changePhaser = Phaser(allPeers - 2)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val signalListener = mapOf(
            Signal.PigPaxosLeaderElected to SignalListener {
                logger.info("Arrived at election ${it.subject.getPeerName()}")
                electionPhaser.arrive()
            },
            Signal.PigPaxosChangeCommitted to SignalListener {
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

        val peerAddresses = apps.getRunningPeers(peerset(0).peersetId).values

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val firstLeaderAddress = getLeaderAddress(peers[0])

        val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(2)
        peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
        val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
        val change = createChange(null)

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first().address}/v2/change/sync?peerset=peerset0", change)
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
        val allPeers = 5

        val electionPhaser = Phaser(1)
        val changePhaser = Phaser(allPeers - 4)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener { electionPhaser.arrive() }
        val peerApplyChange = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            changePhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.PigPaxosLeaderElected to peerLeaderElected,
            Signal.PigPaxosReceivedAccept to peerApplyChange,
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers(peerset(0).peersetId).values

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val leaderAddressMap = peers.map { getLeaderAddress(it) }.groupingBy { it }.eachCount()

        var firstLeaderAddress = leaderAddressMap.toList().first().first

        leaderAddressMap.forEach {
            if(it.value > leaderAddressMap[firstLeaderAddress]!!) firstLeaderAddress = it.key
        }

        logger.info("FirstLeaderAddress $firstLeaderAddress leaderAddressMap: $leaderAddressMap")


        val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(3)
        peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
        val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
        val change = createChange(null)

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first().address}/v2/change/async?peerset=peerset0", change)
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
        var allPeers = 5

        var isNetworkDivided = false

        val election1Phaser = Phaser(1)
        allPeers -= 2
        val election2Phaser = Phaser(1)
        val change1Phaser = Phaser(3)
        val change2Phaser = Phaser(2)
        val abortChangePhaser = Phaser(5)
        listOf(election1Phaser, election2Phaser, change1Phaser, change2Phaser, abortChangePhaser).forEach { it.register() }

        val signalListener = mapOf(
            Signal.PigPaxosLeaderElected to SignalListener {
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
            Signal.PigPaxosChangeCommitted to SignalListener {
                if (change1Phaser.phase == 0) {
                    logger.info("Arrived at change 1 ${it.subject.getPeerName()}")
                    change1Phaser.arrive()
                } else {
                    logger.info("Arrived at change 2 ${it.subject.getPeerName()}")
                    change2Phaser.arrive()
                }
            },
            Signal.PigPaxosChangeAborted to SignalListener{
                logger.info("Arrived at abortChange ${it.subject.getPeerName()}")
                abortChangePhaser.arrive()
            },
        )

        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
            ),
            signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
        )
        val peers = apps.getRunningApps()

        val peerAddresses = apps.getRunningPeers(peerset(0).peersetId).values
        val peerAddresses2 = apps.getRunningPeers(peerset(0).peersetId)

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("First election finished")

        val firstLeaderAddress = peers.firstNotNullOf { getLeaderAddress(it) }

        logger.info("First leader: $firstLeaderAddress")

        val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

        val firstHalf: List<PeerAddress> = listOf(firstLeaderAddress, notLeaderPeers.first())
        val secondHalf: List<PeerAddress> = notLeaderPeers.drop(1)

//      Divide network
        isNetworkDivided = true

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
        
        logger.info("Second election finished")

        val change1 = createChange(1)
        val change2 = createChange(2)

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first().address}/v2/change/async?peerset=peerset0", change1)
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first().address}/v2/change/async?peerset=peerset0", change2)
        }.isSuccess()

        change1Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 1")

        firstHalf.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            logger.debug("Checking changes $it proposed: $proposedChanges accepted: $acceptedChanges")
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
            logger.info("Checking ${it.peerId} proposed: $proposedChanges accepted: $acceptedChanges")
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
    fun `should synchronize on history if it was added outside of paxos`(): Unit = runBlocking {
        val phaserGPACPeer = Phaser(1)
        val phaserPigPaxosPeers = Phaser(5)
        val leaderElectedPhaser = Phaser(1)

        val isSecondGPAC = AtomicBoolean(false)

        listOf(phaserGPACPeer, phaserPigPaxosPeers, leaderElectedPhaser).forEach { it.register() }

        val change1 = AddGroupChange(
            "name",
            peersets = listOf(ChangePeersetInfo(peerset(0), InitialHistoryEntry.getId())),
        )

        val change2 =
            AddGroupChange(
                "name", peersets =
                listOf(
                    ChangePeersetInfo(
                        peerset(0),
                        change1.toHistoryEntry(peerset(0)).getId()
                    )
                )
            )


        val peerGPACAction = SignalListener {
            phaserGPACPeer.arrive()
        }

        val leaderElectedAction = SignalListener { leaderElectedPhaser.arrive() }

        val firstLeaderAction = SignalListener { signalData ->
            val url = "http://${signalData.peerResolver.resolve(peerId(1)).address}/apply?peerset=peerset0"
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

        val consensusPeersAction = SignalListener {
            logger.info("Arrived: ${it.change}")
            if(it.change == change2) phaserPigPaxosPeers.arrive()
        }

        val firstPeerSignals = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction,
            Signal.PigPaxosLeaderElected to leaderElectedAction,
            Signal.PigPaxosChangeCommitted to consensusPeersAction,
            Signal.OnHandlingElectBegin to SignalListener {
                if (isSecondGPAC.get()) {
                    throw Exception("Ignore restarting GPAC")
                }
            }
        )

        val peerSignals =
            mapOf(
                Signal.PigPaxosLeaderElected to leaderElectedAction,
                Signal.PigPaxosChangeCommitted to consensusPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") }
            )

        val peerPaxosSignals =
            mapOf(
                Signal.PigPaxosLeaderElected to leaderElectedAction,
                Signal.PigPaxosChangeCommitted to consensusPeersAction,
                Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                Signal.OnHandlingAgreeBegin to SignalListener { throw Exception("Ignore GPAC") }
            )

        val peer1Signals =
            mapOf(
                Signal.PigPaxosLeaderElected to leaderElectedAction,
                Signal.PigPaxosChangeCommitted to consensusPeersAction,
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
                peer(3) to peerPaxosSignals,
                peer(4) to peerPaxosSignals,
            ), configOverrides = mapOf(
                peer(0) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(1) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(2) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(3) to mapOf("gpac.maxLeaderElectionTries" to 2),
                peer(4) to mapOf("gpac.maxLeaderElectionTries" to 2),
            )
        )

        leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

        // change committed on 3/5 peers
        try {
            executeChange(
                "${apps.getPeer(peer(0)).address}/v2/change/sync?peerset=peerset0&enforce_gpac=true",
                change1
            )
            fail("Change passed")
        } catch (e: Exception) {
            logger.info("Leader 1 fails", e)
        }


        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
        isSecondGPAC.set(true)

        val change = testHttpClient.get<Change>("http://${apps.getPeer(peer(1)).address}/change?peerset=peerset0") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        expect {
            that(change).isA<AddGroupChange>()
            that((change as AddGroupChange).groupName).isEqualTo(change1.groupName)
        }

        executeChange(
            "${apps.getPeer(peer(1)).address}/v2/change/sync?peerset=peerset0",
            change2
        )

        phaserPigPaxosPeers.arriveAndAwaitAdvanceWithTimeout()

        apps.getRunningPeers(peerset(0).peersetId).forEach { (_, peerAddress) ->
            // and should not execute this change couple of times
            val changes = testHttpClient.get<Changes>("http://${peerAddress.address}/changes?peerset=peerset0") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            // only one change and this change shouldn't be applied two times
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


    @Disabled("Not supported for now")
    @Test
    fun `consensus on multiple peersets`(): Unit = runBlocking {
        apps = TestApplicationSet(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                "peerset1" to listOf("peer1", "peer2", "peer4"),
                "peerset2" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                "peerset3" to listOf("peer2", "peer3"),
            ),
        )

        val peersetCount = apps.getPeersets().size
        repeat(peersetCount) { i ->
            logger.info("Sending changes to peerset$i")

            val change1 = createChange(null, peersetId = "peerset$i")
            val parentId = change1.toHistoryEntry(PeersetId("peerset$i")).getId()
            val change2 = createChange(null, peersetId = "peerset$i", parentId = parentId)

            val peerAddress = apps.getPeerAddresses("peerset$i").values.iterator().next().address

            expectCatching {
                executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change1)
                executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change2)
            }.isSuccess()
        }

        repeat(peersetCount) { i ->
            val peerAddress = apps.getPeerAddresses("peerset$i").values.iterator().next()

            val changes = askForChanges(peerAddress)
            expectThat(changes.size).isEqualTo(2)
        }
    }

    private fun createChange(
        acceptNum: Int?,
        userName: String = "userName",
        parentId: String = InitialHistoryEntry.getId(),
        peersetId: String = "peerset0",
    ) = AddUserChange(
        userName,
        acceptNum,
        peersets = listOf(ChangePeersetInfo(PeersetId(peersetId), parentId)),
    )


    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>("http://${uri}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun genericAskForChange(suffix: String, peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/pigpaxos/$suffix?peerset=peerset0") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }


    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/v2/change?peerset=peerset0") {
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
        val leaderId = (app.getPeersetProtocols(PeersetId("peerset0")).consensusProtocol as PigPaxosProtocol).getLeaderId()
        return leaderId?.let { apps.getPeer(it).address }
    }

    private fun getLeaderAddress(app: ApplicationUcac): PeerAddress? {
        val leaderId = (app.getPeersetProtocols(PeersetId("peerset0")).consensusProtocol as PigPaxosProtocol).getLeaderId()
        return leaderId?.let { apps.getPeer(it) }
    }

    private fun peer(peerId: Int): String = "peer$peerId"
    private fun peerId(peerId: Int): PeerId = PeerId(peer(peerId))
    private fun peerset(peersetId: Int): PeersetId = PeersetId("peerset$peersetId")

    companion object {
        private val logger = LoggerFactory.getLogger(PigPaxosSpec::class.java)
    }
}
