package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.utils.TestApplicationSet
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.*
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

typealias LeaderAddressPortAndApplication = Triple<String, String, Application>


class ConsensusSpec {

    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp(testInfo: TestInfo) {
        System.setProperty("configFile", "consensus_application.conf")
        println("\n\n${testInfo.displayName}")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {
        //1. happy-path, wszyscy żyją i jeden zostaje wybrany jako leader
        //* peer 1 wysyła prośbę o głosowanie na niego
        //* peer 1 dostaje większość głosów
        //* peer 1 informuje że jest leaderem
        //* peer 1 proponuje zmianę (akceptowana)
        //* peer 2 proponuje zmianę (akceptowana)

        val peersWithoutLeader = 4


//      TODO: Change to one phaser with use of advance -> will be done in next PR
        val electionPhaser = Phaser(peersWithoutLeader)
        val change1Phaser = Phaser(peersWithoutLeader)
        val change2Phaser = Phaser(peersWithoutLeader)

        listOf(electionPhaser, change1Phaser, change2Phaser).forEach { it.register() }

        val peerLeaderElected = SignalListener {
            electionPhaser.arrive()
        }

        val peerApplyChange = SignalListener {
            println("Change phaser: ${it.subject.getPeerName()}")
            if (change1Phaser.phase == 0) change1Phaser.arrive() else change2Phaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeAccepted to peerApplyChange
        )
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners = signalListeners)
        val peerAddresses = peerset.getPeers()[0]

        awaitAdvanceInterruptiblyPhaser(electionPhaser)

        // when: peer1 executed change
        val change1 = createChange(null)
        val change1Id = change1.toHistoryEntry().getId()
        expectCatching {
            executeChange("${peerAddresses[0]}/consensus/create_change/sync", change1)
        }.isSuccess()

        awaitAdvanceInterruptiblyPhaser(change1Phaser)

        val changes = askForChanges(peerAddresses[2])

        // then: there's one change and it's change we've requested
        expect {
            that(changes.size).isEqualTo(1)
            that(changes[0]).isEqualTo(change1)
            that(changes[0].acceptNum).isEqualTo(null)
        }


        // when: peer2 executes change
        val change2 = createChange(1, userName = "userName2", parentId = change1Id)
        expectCatching {
            executeChange("${peerAddresses[1]}/consensus/create_change/sync", change2)
        }.isSuccess()

        awaitAdvanceInterruptiblyPhaser(change2Phaser)

        val changes2 = askForChanges(peerAddresses[2])

        // then: there are two changes
        expect {
            that(changes2.size).isEqualTo(2)
            that(changes2[1]).isEqualTo(change2)
            that(changes2[1].acceptNum).isEqualTo(1)
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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }
        val peerset =
            TestApplicationSet(1, listOf(5), appsToExclude = listOf(3, 4, 5), signalListeners = signalListeners)

        awaitAdvanceInterruptiblyPhaser(phaser)

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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), appsToExclude = listOf(4, 5), signalListeners = signalListeners)

        awaitAdvanceInterruptiblyPhaser(phaser)
        isLeaderElected = true

        peerset.getRunningApps().forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
                println("For ${it.getBoundPort()} leader is $leaderAddress")
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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners = signalListeners)
        var apps = peerset.getRunningApps()

        awaitAdvanceInterruptiblyPhaser(election1Phaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        apps = apps.filter { it != firstLeaderApplication }

        awaitAdvanceInterruptiblyPhaser(election2Phaser)

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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), appsToExclude = listOf(5), signalListeners = signalListeners)
        var apps = peerset.getRunningApps()

        awaitAdvanceInterruptiblyPhaser(election1Phaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        apps = apps.filter { it != firstLeaderApplication }

        awaitAdvanceInterruptiblyPhaser(election2Phaser)

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
        val triesToBecomeLeader = 3

        val leaderFailedPhaser = Phaser(peersWithoutLeader)
        val electionPhaser = Phaser(peersWithoutLeader)
        val tryToBecomeLeaderPhaser = Phaser(activePeers * triesToBecomeLeader)

        listOf(leaderFailedPhaser, electionPhaser, tryToBecomeLeaderPhaser).forEach { it.register() }

        val peerTryToBecomeLeader = SignalListener { tryToBecomeLeaderPhaser.arrive() }

        val peerLeaderFailed = SignalListener { leaderFailedPhaser.arrive() }
        val peerLeaderElected = SignalListener { electionPhaser.arrive() }

        val signalListener = mapOf(
            Signal.ConsensusLeaderDoesNotSendHeartbeat to peerLeaderFailed,
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
        )
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..6).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(6), appsToExclude = listOf(5, 6), signalListeners = signalListeners)
        var apps = peerset.getRunningApps()

        awaitAdvanceInterruptiblyPhaser(electionPhaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        apps = apps.filter { it != firstLeaderApplication }

        awaitAdvanceInterruptiblyPhaser(leaderFailedPhaser)
        awaitAdvanceInterruptiblyPhaser(tryToBecomeLeaderPhaser, 20)

        expect {
            val secondLeaderAddress = askForLeaderAddress(apps.first())
            that(secondLeaderAddress).isEqualTo(noneLeader)
        }

        peerset.stopApps()
    }


    @Test
    fun `leader fails during processing change`(): Unit = runBlocking {
        val testName = "leader fails during processing change"
        var peersWithoutLeader = 4

        val election1Phaser = Phaser(peersWithoutLeader)
        peersWithoutLeader -= 1
        val election2Phaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader)
        var shouldElection2Starts = false
        listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
        var firstLeader = true

        val leaderAction = SignalListener {
            if(firstLeader) throw RuntimeException("Failed after proposing change")
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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners = signalListeners)
        val apps = peerset.getRunningApps()

        awaitAdvanceInterruptiblyPhaser(election1Phaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

//      Start processing
        expectCatching {
            executeChange("$firstLeaderAddress/consensus/create_change/sync", createChange(null))
        }.isFailure()
        firstLeader = false

        firstLeaderApplication.stop(0, 0)
        shouldElection2Starts = true

        val runningPeers = peerset.getRunningPeers()[0].filterNot { it.contains(firstLeaderPort) }

        expect {
            val proposedChanges = askForProposedChanges(runningPeers.first())
            val acceptedChanges = askForAcceptedChanges(runningPeers.first())
            that(proposedChanges.size).isEqualTo(1)
            that(proposedChanges.first()).isEqualTo(createChange(null))
            that(proposedChanges.first().acceptNum).isEqualTo(null)
            that(acceptedChanges.size).isEqualTo(0)
        }

        awaitAdvanceInterruptiblyPhaser(election2Phaser)
        awaitAdvanceInterruptiblyPhaser(changePhaser)

        expect {
            val proposedChanges = askForProposedChanges(runningPeers.first())
            that(proposedChanges.size).isEqualTo(0)
            val acceptedChanges = askForAcceptedChanges(runningPeers.first())
            that(acceptedChanges.size).isEqualTo(1)
            that(acceptedChanges.first()).isEqualTo(createChange(null))
            that(acceptedChanges.first().acceptNum).isEqualTo(null)
        }

        peerset.stopApps()
    }

    @Test
    fun `less than half of peers fails after electing leader`(): Unit = runBlocking {
        val testName = "less than half of peers fails after electing leader"
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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners = signalListeners)
        val apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        awaitAdvanceInterruptiblyPhaser(electionPhaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        val peersToStop = peerAddresses.zip(apps).filterNot { it.first.contains(firstLeaderPort) }.take(2)
        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(apps).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/consensus/create_change/sync", createChange(null))
        }.isSuccess()

        awaitAdvanceInterruptiblyPhaser(changePhaser)

        runningPeers.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(null)
            }

        }

        peerset.stopApps()
    }

    @Test
    fun `more than half of peers fails during propagating change`(): Unit = runBlocking {
        val testName = "more than half of peers fails during propagating change"
        val peersWithoutLeader = 4

        val electionPhaser = Phaser(peersWithoutLeader)
        val changePhaser = Phaser(peersWithoutLeader - 3)
        listOf(electionPhaser, changePhaser).forEach { it.register() }

        val peerLeaderElected = SignalListener { electionPhaser.arrive() }
        val peerApplyChange = SignalListener {
            println("Arrived $it")
            changePhaser.arrive()
        }

        val signalListener = mapOf(
            Signal.ConsensusLeaderElected to peerLeaderElected,
            Signal.ConsensusFollowerChangeProposed to peerApplyChange
        )
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners = signalListeners)
        var apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        awaitAdvanceInterruptiblyPhaser(electionPhaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        val peersToStop = peerAddresses.zip(apps).filterNot { it.first.contains(firstLeaderPort) }.take(3)
        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(apps).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/consensus/create_change/sync", createChange(null))
        }.isSuccess()

        awaitAdvanceInterruptiblyPhaser(changePhaser)

//      As only one peer confirm changes it should be still proposedChange
        runningPeers.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(1)
                that(proposedChanges.first()).isEqualTo(createChange(null))
                that(proposedChanges.first().acceptNum).isEqualTo(null)
                that(acceptedChanges.size).isEqualTo(0)
            }
        }

        peerset.stopApps()
    }

    @Test
    fun `network divide on half and then merge`(): Unit = runBlocking {
        val testName = "network divide on half and then merge"
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
        val signalListeners: Map<Int, Map<Signal, SignalListener>> = (0..5).associateWith { signalListener }

        val peerset = TestApplicationSet(1, listOf(5), signalListeners)
        var apps = peerset.getRunningApps()

        val peerAddresses = peerset.getRunningPeers()[0]

        awaitAdvanceInterruptiblyPhaser(election1Phaser)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(apps)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

        val firstHalf = listOf(firstLeaderAddress, notLeaderPeers.first())
        val secondHalf = notLeaderPeers.drop(1)

        val addressToApplication: Map<String, Application> = peerAddresses.zip(apps).toMap()


//      Divide network
        println("${firstLeaderPort}-${firstLeaderAddress} -> old leader")
        isNetworkDivided = true


        firstHalf.forEach { address ->
            val application = addressToApplication[address]
            val peers = firstHalf.filter { it != address } + secondHalf.map { it.replace(knownPeerIp, unknownPeerIp) }
            modifyPeers(application!!, peers)
        }

        secondHalf.forEach { address ->
            val application = addressToApplication[address]
            val peers = secondHalf.filter { it != address } + firstHalf.map { it.replace(knownPeerIp, unknownPeerIp) }
            modifyPeers(application!!, peers)
        }

        awaitAdvanceInterruptiblyPhaser(election2Phaser)
//      Check if second half chose new leader
        secondHalf.forEach {
            val app = addressToApplication[it]
            val newLeaderAddress = askForLeaderAddress(app!!)
            println("$it -> $newLeaderAddress oldLeaderAddress: $firstLeaderAddress")
            expectThat(newLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first()}/consensus/create_change/sync", createChange(1))
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first()}/consensus/create_change/sync", createChange(2))
        }.isSuccess()

        awaitAdvanceInterruptiblyPhaser(change1Phaser)

        firstHalf.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(1)
                that(proposedChanges.first()).isEqualTo(createChange(null))
                that(proposedChanges.first().acceptNum).isEqualTo(1)
                that(acceptedChanges.size).isEqualTo(0)
            }
        }

        secondHalf.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }


//      Merge network
        peerAddresses.forEach { address ->
            val application = addressToApplication[address]
            modifyPeers(application!!, peerAddresses.filter { it != address })
        }

        awaitAdvanceInterruptiblyPhaser(change2Phaser)

        peerAddresses.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
                that(acceptedChanges.first()).isEqualTo(createChange(null))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }

        peerset.stopApps()
    }

    @Test
    fun `unit tests of isMoreThanHalf() function`(): Unit = runBlocking {
        val listOfPeers = mutableListOf("2", "3")

        val initializeConsensusProtocol = { otherPeers: List<String> ->
            RaftConsensusProtocolImpl(
                0,
                0,
                "1",
                Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
                otherPeers,
                protocolClient = RaftProtocolClientImpl(0)
            )
        }

        val consensus = initializeConsensusProtocol(listOfPeers)
        expectThat(consensus.isMoreThanHalf(0)).isFalse()
        expectThat(consensus.isMoreThanHalf(1)).isTrue()
        expectThat(consensus.isMoreThanHalf(2)).isTrue()

        listOfPeers.add("4")
        consensus.setOtherPeers(listOfPeers)
        expectThat(consensus.isMoreThanHalf(0)).isFalse()
        expectThat(consensus.isMoreThanHalf(1)).isFalse()
        expectThat(consensus.isMoreThanHalf(2)).isTrue()
        expectThat(consensus.isMoreThanHalf(3)).isTrue()

        listOfPeers.add("5")
        consensus.setOtherPeers(listOfPeers)
        expectThat(consensus.isMoreThanHalf(0)).isFalse()
        expectThat(consensus.isMoreThanHalf(1)).isFalse()
        expectThat(consensus.isMoreThanHalf(2)).isTrue()
        expectThat(consensus.isMoreThanHalf(3)).isTrue()
        expectThat(consensus.isMoreThanHalf(4)).isTrue()

        listOfPeers.add("6")
        consensus.setOtherPeers(listOfPeers)
        expectThat(consensus.isMoreThanHalf(0)).isFalse()
        expectThat(consensus.isMoreThanHalf(1)).isFalse()
        expectThat(consensus.isMoreThanHalf(2)).isFalse()
        expectThat(consensus.isMoreThanHalf(3)).isTrue()
        expectThat(consensus.isMoreThanHalf(4)).isTrue()
        expectThat(consensus.isMoreThanHalf(5)).isTrue()
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

    private suspend fun genericAskForChange(suffix: String, peer: String): List<Change> =
        testHttpClient.get<Changes>("http://$peer/consensus/$suffix") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }


    private suspend fun askForChanges(peer: String) = genericAskForChange("changes", peer)
    private suspend fun askForProposedChanges(peer: String) = genericAskForChange("proposed_changes", peer)
    private suspend fun askForAcceptedChanges(peer: String) = genericAskForChange("accepted_changes", peer)

    private suspend fun askForLeaderAddress(app: Application): String? {
        val consensusProperty =
            Application::class.declaredMemberProperties.single { it.name == "consensusProtocol" }
        val consensusOldAccessible = consensusProperty.isAccessible
        try {
            consensusProperty.isAccessible = true
            val consensusProtocol = consensusProperty.get(app) as RaftConsensusProtocolImpl
            return consensusProtocol.getLeaderAddress()
        } finally {
            consensusProperty.isAccessible = consensusOldAccessible
        }
    }

    private suspend fun getLeaderAddressPortAndApplication(peers: List<Application>): LeaderAddressPortAndApplication {
        val peerAddresses = getPeerAddresses(peers)
        val address =
            askForLeaderAddress(peers[0])!!

        expect {
            that(address).isNotEqualTo(noneLeader)
        }

        val port = getPortFromAddress(address)

        val addressAndApplication = peerAddresses.zip(peers).first { it.first.contains(port) }

        val leaderAddress = addressAndApplication.first.replace("127.0.0.1", "localhost")
        val application = addressAndApplication.second

        return Triple(leaderAddress, port, application)
    }

    private fun getPeerAddresses(apps: List<Application>): List<String> =
        apps.map { "127.0.0.1:${it.getBoundPort()}" }

    private fun getPortFromAddress(address: String) = address.split(":")[1]

    private fun modifyPeers(app: Application, peers: List<String>) {
        val newPeers = peers.map { it.replace("http://", "") }
        app.setPeers(mapOf(1 to newPeers), "127.0.0.1")
    }

    private suspend fun awaitAdvanceInterruptiblyPhaser(phaser: Phaser, secondsTimeout: Long = 15) =
        withContext(Dispatchers.IO) {
            phaser.awaitAdvanceInterruptibly(phaser.arrive(), secondsTimeout, TimeUnit.SECONDS)
        }


}
