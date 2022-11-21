package com.github.davenury.ucac.consensus

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.*
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

typealias LeaderAddressPortAndApplication = Triple<String, String, ApplicationUcac>

@ExtendWith(TestLogExtension::class)
class ConsensusSpec {

    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "consensus_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val phaser = Phaser(peersWithoutLeader)
        phaser.register()

        val peerLeaderElected = SignalListener {
            expectThat(phaser.phase).isEqualTo(0)
            phaser.arrive()
        }

        val peerApplyChange = SignalListener {
            expectThat(phaser.phase).isContainedIn(listOf(1, 2))
            phaser.arrive()
        }

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeAccepted to peerApplyChange
                )
            }
        )
        val peerAddresses = peerset.getPeers()[0]

        phaser.arriveAndAwaitAdvanceWithTimeout()

        // when: peer1 executed change
        val change1 = createChange(null)
        val change1Id = change1.toHistoryEntry().getId()
        expectCatching {
            executeChange("${peerAddresses[0]}/v2/change/sync", change1)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peerAddresses).forEach { changes ->
            // then: there's one change, and it's change we've requested
            expectThat(changes.size).isEqualTo(1)
            expect {
                that(changes[0]).isEqualTo(change1)
                that(changes[0].acceptNum).isEqualTo(null)
            }
        }

        // when: peer2 executes change
        val change2 = createChange(1, userName = "userName2", parentId = change1Id)
        expectCatching {
            executeChange("${peerAddresses[1]}/v2/change/sync", change2)
        }.isSuccess()

        phaser.arriveAndAwaitAdvanceWithTimeout()

        askAllForChanges(peerAddresses).forEach { changes ->
            // then: there are two changes
            expectThat(changes.size).isEqualTo(2)
            expect {
                that(changes[1]).isEqualTo(change2)
                that(changes[0]).isEqualTo(change1)
                that(changes[1].acceptNum).isEqualTo(1)
            }
        }

        peerset.stopApps()
    }

    @Test
    fun `less than half of peers respond on ConsensusElectMe`(): Unit = runBlocking {
        val activePeers = 2
        val triesToBecomeLeader = 2
        val phaser = Phaser(activePeers * triesToBecomeLeader)

        val peerTryToBecomeLeader = SignalListener {
            phaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
        )
        val peerset = TestApplicationSet(
            listOf(5),
            appsToExclude = listOf(2, 3, 4),
            signalListeners = (0..4).associateWith { signalListener },
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()

        peerset.getRunningApps().forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
//              DONE  it should always be noneLeader
                that(leaderAddress).isEqualTo(noneLeader)
            }
        }

        peerset.stopApps()
    }

    @Test
    fun `minimum number of peers respond on ConsensusElectMe`(): Unit = runBlocking {
        val peersWithoutLeader = 3
        val phaser = Phaser(peersWithoutLeader)
        var isLeaderElected = false

        val peerLeaderElected = SignalListener {
            if (isLeaderElected.not()) phaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
        )

        val peerset = TestApplicationSet(
            listOf(5),
            appsToExclude = listOf(3, 4),
            signalListeners = (0..4).associateWith { signalListener },
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()
        isLeaderElected = true

        peerset.getRunningApps().forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
                that(leaderAddress).isNotEqualTo(noneLeader)
            }
        }

        peerset.stopApps()
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

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        var apps = peerset.getRunningApps()

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        apps = apps.filter { it != firstLeaderApplication }

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress =
                askForLeaderAddress(apps.first())
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

        peerset.stopApps()
    }


    @Test
    fun `less than half peers failed`(): Unit = runBlocking {
        val peersWithoutLeader = 3

        val election1Phaser = Phaser(peersWithoutLeader)
        val election2Phaser = Phaser(peersWithoutLeader - 1)
        listOf(election1Phaser, election2Phaser).forEach { it.register() }

        val peerLeaderElected = SignalListener {
            if (election1Phaser.phase == 0) election1Phaser.arrive() else election2Phaser.arrive()
        }

        val signalListener = mapOf(Signal.ConsensusLeaderElected to peerLeaderElected)

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
            appsToExclude = listOf(4),
        )
        var apps = peerset.getRunningApps()

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        apps = apps.filter { it != firstLeaderApplication }

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress =
                askForLeaderAddress(apps.first())
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

        peerset.stopApps()
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
                tryToBecomeLeaderPhaser.arrive()
            }
        }

        val peerLeaderFailed = SignalListener { leaderFailedPhaser.arrive() }
        val peerLeaderElected = SignalListener { electionPhaser.arrive() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderDoesNotSendHeartbeat to peerLeaderFailed,
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
        )

        val peerset = TestApplicationSet(
            listOf(6),
            appsToExclude = listOf(4, 5),
            signalListeners = (0..5).associateWith { signalListener },
        )
        var apps = peerset.getRunningApps()

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third

        firstLeaderApplication.stop(0, 0)
        leaderElect = true

        apps = apps.filter { it != firstLeaderApplication }

        leaderFailedPhaser.arriveAndAwaitAdvanceWithTimeout()
        tryToBecomeLeaderPhaser.arriveAndAwaitAdvanceWithTimeout()

        expect {
            val secondLeaderAddress = askForLeaderAddress(apps.first())
            that(secondLeaderAddress).isEqualTo(noneLeader)
        }

        peerset.stopApps()
    }

    @Test
    fun `leader fails during processing change`(): Unit = runBlocking {
        var peersWithoutLeader = 4

        val failurePhaser = Phaser(2)
        val election1Phaser = Phaser(peersWithoutLeader)
        peersWithoutLeader -= 1
        val election2Phaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader)
        var shouldElection2Starts = false
        listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
        var firstLeader = true

        val leaderAction = SignalListener {
            if (firstLeader) {
                failurePhaser.arrive()
                throw RuntimeException("Failed after proposing change")
            }
        }

        val peerLeaderElected =
            SignalListener {
                when {
                    election1Phaser.phase == 0 -> election1Phaser.arrive()
                    shouldElection2Starts -> election2Phaser.arrive()
                }
            }

        val peerApplyChange = SignalListener { changePhaser.arrive() }

        val signalListener = mapOf(
            Signal.ConsensusAfterProposingChange to leaderAction,
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeAccepted to peerApplyChange
        )

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        val apps = peerset.getRunningApps()

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

//      Start processing
        val change = createChange(null)
        expectCatching {
            executeChange("$firstLeaderAddress/v2/change/sync?timeout=PT0.1S", change)
        }.isFailure()

        failurePhaser.arriveAndAwaitAdvanceWithTimeout()
        firstLeader = false

        firstLeaderApplication.stop(0, 0)
        shouldElection2Starts = true

        val runningPeers = peerset.getRunningPeers()[0].filterNot { it.contains(firstLeaderPort) }

        val proposedChanges1 = askForProposedChanges(runningPeers.first())
        val acceptedChanges1 = askForAcceptedChanges(runningPeers.first())
        expect{
            that(proposedChanges1.size).isEqualTo(1)
            that(acceptedChanges1.size).isEqualTo(0)
        }
        expect {
            that(proposedChanges1.first()).isEqualTo(change)
            that(proposedChanges1.first().acceptNum).isEqualTo(null)
        }

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()
        changePhaser.arriveAndAwaitAdvanceWithTimeout()

        val proposedChanges2 = askForProposedChanges(runningPeers.first())
        val acceptedChanges2 = askForAcceptedChanges(runningPeers.first())
        expect {
            that(proposedChanges2.size).isEqualTo(0)
            that(acceptedChanges2.size).isEqualTo(1)
        }
        expect {
            that(acceptedChanges2.first()).isEqualTo(change)
            that(acceptedChanges2.first().acceptNum).isEqualTo(null)
        }

        peerset.stopApps()
    }

    @Test
    fun `less than half of peers fails after electing leader`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val electionPhaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader - 2)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener { electionPhaser.arrive() }
        val peerApplyChange = SignalListener { changePhaser.arrive() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeAccepted to peerApplyChange
        )

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        val apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderPort = triple.second

        val peersToStop = peerAddresses.zip(apps).filterNot { it.first.contains(firstLeaderPort) }.take(2)
        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(apps).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/v2/change/sync", createChange(null))
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
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(null)
            }

        }

        peerset.stopApps()
    }

    @Test
    fun `more than half of peers fails during propagating change`(): Unit = runBlocking {
        val peersWithoutLeader = 4

        val electionPhaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader - 3)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener { electionPhaser.arrive() }
        val peerApplyChange = SignalListener {
            changePhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeProposed to peerApplyChange,
        )

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        val apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderPort = triple.second

        val peersToStop = peerAddresses.zip(apps).filterNot { it.first.contains(firstLeaderPort) }.take(3)
        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(apps).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/v2/change/async", createChange(null))
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
                that(proposedChanges.first()).isEqualTo(createChange(null))
                that(proposedChanges.first().acceptNum).isEqualTo(null)
            }
        }

        peerset.stopApps()
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

        val peerLeaderElected =
            SignalListener {
                when {
                    election1Phaser.phase == 0 -> election1Phaser.arrive()
                    isNetworkDivided && election2Phaser.phase == 0 -> election2Phaser.arrive()
                }
            }
        val peerApplyChange =
            SignalListener { if (change1Phaser.phase == 0) change1Phaser.arrive() else change2Phaser.arrive() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeAccepted to peerApplyChange
        )

        val peerset = TestApplicationSet(
            listOf(5),
            signalListeners = (0..4).associateWith { signalListener },
        )
        val apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        election1Phaser.arriveAndAwaitAdvanceWithTimeout()

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderAddress = triple.first

        val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

        val firstHalf = listOf(firstLeaderAddress, notLeaderPeers.first())
        val secondHalf = notLeaderPeers.drop(1)

        val addressToApplication: Map<String, ApplicationUcac> = peerAddresses.zip(apps).toMap()
//      Divide network
        isNetworkDivided = true

        firstHalf.forEach { address ->
            val peers = peerAddresses.map { peer ->
                if (secondHalf.contains(peer)) {
                    peer.replace(knownPeerIp, unknownPeerIp)
                } else {
                    peer
                }
            }
            addressToApplication[address]!!.setPeers(listOf(peers))
        }

        secondHalf.forEach { address ->
            val peers = peerAddresses.map { peer ->
                if (firstHalf.contains(peer)) {
                    peer.replace(knownPeerIp, unknownPeerIp)
                } else {
                    peer
                }
            }
            addressToApplication[address]!!.setPeers(listOf(peers))
        }

        logger.info("Network divided")

        election2Phaser.arriveAndAwaitAdvanceWithTimeout()
//      Check if second half chose new leader
        secondHalf.forEach {
            val app = addressToApplication[it]
            val newLeaderAddress = askForLeaderAddress(app!!)
            expectThat(newLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first()}/v2/change/async", createChange(1))
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first()}/v2/change/sync", createChange(2))
        }.isSuccess()

        change1Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 1")

        firstHalf.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            expect {
                that(proposedChanges.size).isEqualTo(1)
                that(acceptedChanges.size).isEqualTo(0)
            }
            expect {
                that(proposedChanges.first()).isEqualTo(createChange(null))
                that(proposedChanges.first().acceptNum).isEqualTo(1)
            }
        }

        secondHalf.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            expect {
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
            }
            expect {
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }

//      Merge network
        peerAddresses.forEach { address ->
            addressToApplication[address]!!.setPeers(listOf(peerAddresses))
        }

        logger.info("Network merged")

        change2Phaser.arriveAndAwaitAdvanceWithTimeout()

        logger.info("After change 2")

        peerAddresses.forEach {
            val proposedChanges = askForProposedChanges(it)
            val acceptedChanges = askForAcceptedChanges(it)
            expect {
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
            }
            expect {
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }

        peerset.stopApps()
    }

    @Test
    fun `unit tests of isMoreThanHalf() function`(): Unit = runBlocking {
        val listOfPeers = mutableListOf("1", "2", "3")

        val peerResolver = PeerResolver(GlobalPeerId(0, 0), listOf<List<String>>(listOfPeers))
        val consensus = RaftConsensusProtocolImpl(
            History(),
            "1",
            Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
            peerResolver,
            protocolClient = RaftProtocolClientImpl(),
        )
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isTrue()
            that(consensus.isMoreThanHalf(2)).isTrue()
        }

        listOfPeers.add("4")
        peerResolver.setPeers(listOf(listOfPeers))
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
        }

        listOfPeers.add("5")
        peerResolver.setPeers(listOf(listOfPeers))
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isTrue()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
        }

        listOfPeers.add("6")
        peerResolver.setPeers(listOf(listOfPeers))
        expect {
            that(consensus.isMoreThanHalf(0)).isFalse()
            that(consensus.isMoreThanHalf(1)).isFalse()
            that(consensus.isMoreThanHalf(2)).isFalse()
            that(consensus.isMoreThanHalf(3)).isTrue()
            that(consensus.isMoreThanHalf(4)).isTrue()
            that(consensus.isMoreThanHalf(5)).isTrue()
        }
    }

    private fun createChange(
        acceptNum: Int?,
        peers: List<String> = listOf(),
        userName: String = "userName",
        parentId: String = InitialHistoryEntry.getId(),
    ) = AddUserChange(
        parentId,
        userName,
        peers,
        acceptNum,
    )

    private suspend fun executeChange(uri: String, change: Change) =
        testHttpClient.post<String>("http://${uri}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun genericAskForChange(suffix: String, peer: String) =
        testHttpClient.get<Changes>("http://$peer/consensus/$suffix") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }


    private suspend fun askForChanges(peer: String) =
        testHttpClient.get<Changes>("http://$peer/v2/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: List<String>) =
        peerAddresses.map { askForChanges(it) }

    private suspend fun askForProposedChanges(peer: String) = genericAskForChange("proposed_changes", peer)
    private suspend fun askForAcceptedChanges(peer: String) = genericAskForChange("accepted_changes", peer)

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

    private suspend fun getLeaderAddressPortAndApplication(peers: List<ApplicationUcac>): LeaderAddressPortAndApplication {
        val peerAddresses = getPeerAddresses(peers)
        val address =
            askForLeaderAddress(peers[0])!!

        expectThat(address).isNotEqualTo(noneLeader)

        val port = getPortFromAddress(address)

        val addressAndApplication = peerAddresses.zip(peers).first { it.first.contains(port) }

        val leaderAddress = addressAndApplication.first.replace("127.0.0.1", "localhost")
        val application = addressAndApplication.second

        return Triple(leaderAddress, port, application)
    }

    private fun getPeerAddresses(apps: List<ApplicationUcac>): List<String> =
        apps.map { "127.0.0.1:${it.getBoundPort()}" }

    private fun getPortFromAddress(address: String) = address.split(":")[1]

    companion object {
        private val logger = LoggerFactory.getLogger(ConsensusSpec::class.java)
    }
}
