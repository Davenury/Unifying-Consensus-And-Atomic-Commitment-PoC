package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.ChangeWithAcceptNum
import com.github.davenury.ucac.common.HistoryDto
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isNotEqualTo
import strikt.assertions.isSuccess
import kotlin.random.Random
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

typealias LeaderAddressPortAndApplication = Triple<String, String, Application>


class ConsensusSpec {

    private val leaderElectionDelay = 7_000L
    private val changePropagationDelay = 7_000L
    private val knownPeerIp = "127.0.0.1"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null


    @BeforeEach
    internal fun setUp() {
        System.setProperty("configFile", "consensus_application.conf")
    }

    @Test
    fun `happy path`(): Unit = runBlocking {

        //1. happy-path, wszyscy żyją i jeden zostaje wybrany jako leader
        //* peer 1 wysyła prośbę o głosowanie na niego
        //* peer 1 dostaje większość głosów
        //* peer 1 informuje że jest leaderem
        //* peer 1 proponuje zmianę (akceptowana)
        //* peer 2 proponuje zmianę (akceptowana)


        val peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }

        peers.forEach { it.startNonblocking() }
        val peerAddresses = getPeerAddresses(peers)
        updatePeersAddresses(peers)

        delay(leaderElectionDelay)

        // when: peer1 executed change
        expectCatching {
            executeChange("${peerAddresses[0]}/consensus/create_change", createChangeWithAcceptNum(null))
        }.isSuccess()

        delay(changePropagationDelay)

        val changes = askForChanges(peerAddresses[2])

        // then: there's one change and it's change we've requested
        expect {
            that(changes.size).isEqualTo(1)
            that(changes[0].change).isEqualTo(AddUserChange("userName"))
            that(changes[0].acceptNum).isEqualTo(null)
        }

        // when: peer2 executes change
        expectCatching {
            executeChange("${peerAddresses[1]}/consensus/create_change", createChangeWithAcceptNum(1))
        }.isSuccess()

        delay(changePropagationDelay)

        val changes2 = askForChanges(peerAddresses[2])

        // then: there are two changes
        expect {
            that(changes2.size).isEqualTo(2)
            that(changes2[1].change).isEqualTo(AddUserChange("userName"))
            that(changes2[1].acceptNum).isEqualTo(1)
        }

        peers.forEach { it.stop(0, 0) }

    }

    @Test
    fun `less than half of peers response on ConsensusElectMe`(): Unit = runBlocking {

        val allPeers =
            (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }

        val peers = allPeers.take(2)

        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers, allPeers.size - peers.size)

        delay(leaderElectionDelay)

        peers.forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
                that(leaderAddress).isEqualTo(noneLeader)
            }
        }

        peers.forEach { it.stop(0, 0) }
    }

    @Test
    fun `minimum number of peers response on ConsensusElectMe`(): Unit = runBlocking {

        val allPeers =
            (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }

        val peers = allPeers.take(3)
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers, allPeers.size - peers.size)

        delay(leaderElectionDelay * 2)

        peers.forEach {
            expect {
                val leaderAddress = askForLeaderAddress(it)
                that(leaderAddress).isNotEqualTo(noneLeader)
            }
        }

        peers.forEach { it.stop(0, 0) }
    }

    @Test
    fun `leader failed and new leader is elected`(): Unit = runBlocking {

        var peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first


        firstLeaderApplication.stop(0, 0)

        peers = peers.filter { it != firstLeaderApplication }

        delay(leaderElectionDelay)

        expect {

            val secondLeaderAddress =
                peers.first().let { askForLeaderAddress(it) }
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

        peers.forEach { it.stop(0, 0) }
    }

    @Test
    fun `less than half peers failed`(): Unit = runBlocking {


        var peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        firstLeaderApplication.stop(0, 0)

        peers = peers.filter { it != firstLeaderApplication }

        delay(leaderElectionDelay)

        expect {

            val secondLeaderAddress =
                peers.first().let { askForLeaderAddress(it) }
            that(secondLeaderAddress).isNotEqualTo(noneLeader)
            that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress)
        }

        peers.forEach { it.stop(0, 0) }
    }


    @Test
    fun `leader fails during processing change`(): Unit = runBlocking {

        val leaderAction = SignalListener {
            throw RuntimeException("Failed after proposing change")
        }

        val signalListeners = mapOf(Signal.ConsensusAfterProposingChange to leaderAction)

        var peers = (1..5).map {
            createApplication(
                arrayOf(it.toString(), "1"),
                signalListeners = signalListeners,
                mode = TestApplicationMode(it, 1)
            )
        }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first


//      Start processing
        expectCatching {
            executeChange("$firstLeaderAddress/consensus/create_change", createChangeWithAcceptNum(null))
        }.isFailure()

        firstLeaderApplication.stop(0, 0)

        peers = peers.filter { it != firstLeaderApplication }

        val runningPeers = peerAddresses.filterNot { it.contains(firstLeaderPort) }

        expect {
            val proposedChanges = askForProposedChanges(runningPeers.first())
            val acceptedChanges = askForAcceptedChanges(runningPeers.first())
            that(proposedChanges.size).isEqualTo(1)
            that(proposedChanges.first().change).isEqualTo(AddUserChange("userName"))
            that(proposedChanges.first().acceptNum).isEqualTo(null)
            that(acceptedChanges.size).isEqualTo(0)

        }

        delay(leaderElectionDelay + changePropagationDelay)

        expect {
            val proposedChanges = askForProposedChanges(runningPeers.first())
            that(proposedChanges.size).isEqualTo(0)
            val acceptedChanges = askForAcceptedChanges(runningPeers.first())
            that(acceptedChanges.size).isEqualTo(1)
            that(acceptedChanges.first().change).isEqualTo(AddUserChange("userName"))
            that(acceptedChanges.first().acceptNum).isEqualTo(null)
        }

        peers.forEach { it.stop(0, 0) }
    }

    @Test
    fun `less than half of peers fails after electing leader`(): Unit = runBlocking {

        var peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first


        val peersToStop = peerAddresses.zip(peers).filterNot { it.first.contains(firstLeaderPort) }.take(2)

        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(peers).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/consensus/create_change", createChangeWithAcceptNum(null))
        }.isSuccess()

        delay(changePropagationDelay)

        runningPeers.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
                that(acceptedChanges.first().change).isEqualTo(AddUserChange("userName"))
                that(acceptedChanges.first().acceptNum).isEqualTo(null)
            }

        }

        peers.forEach { it.stop(0, 0) }
    }

    @Test
    fun `more than half of peers fails during propagating change`(): Unit = runBlocking {

        var peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first


        val peersToStop = peerAddresses.zip(peers).filterNot { it.first.contains(firstLeaderPort) }.take(3)
        peersToStop.forEach { it.second.stop(0, 0) }
        val runningPeersAddressAndApplication = peerAddresses.zip(peers).filterNot { addressAndApplication ->
            val addressesStopped = peersToStop.map { it.first }
            addressesStopped.contains(addressAndApplication.first)
        }

        val runningPeers = runningPeersAddressAndApplication.map { it.first }

//      Start processing
        expectCatching {
            executeChange("${runningPeers.first()}/consensus/create_change", createChangeWithAcceptNum(null))
        }.isSuccess()

        delay(changePropagationDelay)

//      As only one peer confirm changes it should be still proposedChange
        runningPeers.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(1)
                that(proposedChanges.first().change).isEqualTo(AddUserChange("userName"))
                that(proposedChanges.first().acceptNum).isEqualTo(null)
                that(acceptedChanges.size).isEqualTo(0)
            }
        }

        peers.forEach { it.stop(0, 0) }
    }

    @Disabled("WIP")
    @Test
    fun `network divide on half and then merge`(): Unit = runBlocking {


        var peers = (1..5).map { createApplication(arrayOf(it.toString(), "1"), mode = TestApplicationMode(it, 1)) }
        peers.forEach { it.startNonblocking() }
        updatePeersAddresses(peers)
        val peerAddresses = getPeerAddresses(peers)

        delay(leaderElectionDelay)

        val triple: LeaderAddressPortAndApplication = getLeaderAddressPortAndApplication(peers)
        val firstLeaderApplication = triple.third
        val firstLeaderPort = triple.second
        val firstLeaderAddress = triple.first

        val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

        val firstHalf = listOf(firstLeaderAddress, notLeaderPeers.first())
        val secondHalf = notLeaderPeers.drop(1)

        val addressToApplication: Map<String, Application> = peerAddresses.zip(peers).toMap()


//      Divide network
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


//      Delay to chose leader in second half
        delay(leaderElectionDelay)

//      Run change in both halfs
        expectCatching {
            executeChange("${firstHalf.first()}/consensus/create_change", createChangeWithAcceptNum(1))
        }.isSuccess()

        expectCatching {
            executeChange("${secondHalf.first()}/consensus/create_change", createChangeWithAcceptNum(2))
        }.isSuccess()


        delay(changePropagationDelay)


        firstHalf.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(1)
                that(proposedChanges.first().change).isEqualTo(AddUserChange("userName"))
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
                that(acceptedChanges.first().change).isEqualTo(AddUserChange("userName"))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }


//      Merge network
        peerAddresses.forEach { address ->
            val application = addressToApplication[address]
            modifyPeers(application!!, peerAddresses.filter { it != address })
        }

        delay(changePropagationDelay)

        peerAddresses.forEach {
            expect {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                that(proposedChanges.size).isEqualTo(0)
                that(acceptedChanges.size).isEqualTo(1)
                that(acceptedChanges.first().change).isEqualTo(AddUserChange("userName"))
                that(acceptedChanges.first().acceptNum).isEqualTo(2)
            }
        }

        peers.forEach { it.stop(0, 0) }
    }


    private val exampleChange =
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )


    private fun createChangeWithAcceptNum(acceptNum: Int?) = mapOf(
        "change" to exampleChange,
        "acceptNum" to acceptNum
    )


    private suspend fun executeChange(uri: String, requestBody: Map<String, Any?>) =
        testHttpClient.post<String>("http://${uri}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = requestBody
        }

    private suspend fun genericAskForChange(suffix: String, peer: String): List<ChangeWithAcceptNum> =
        testHttpClient.get<String>("http://$peer/consensus/$suffix") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.changeDto.toChange(), it.acceptNum) }


    private suspend fun askForChanges(peer: String) = genericAskForChange("changes", peer)
    private suspend fun askForProposedChanges(peer: String) = genericAskForChange("proposed_changes", peer)
    private suspend fun askForAcceptedChanges(peer: String) = genericAskForChange("accepted_changes", peer)

    private fun askForLeaderAddress(app: Application): String? {
        val consensusProperty =
            Application::class.declaredMemberProperties.single { it.name == "consensusProtocol" }
        val consensusOldAccessible = consensusProperty.isAccessible
        var leaderAddress: String? = null
        try {
            consensusProperty.isAccessible = true
            val consensusProtocol = consensusProperty.get(app) as RaftConsensusProtocolImpl
            leaderAddress = consensusProtocol.getLeaderAddress()
            return leaderAddress
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

        val leaderAddress = addressAndApplication.first
        val application = addressAndApplication.second

        return Triple(leaderAddress, port, application)
    }

    private fun getPeerAddresses(apps: List<Application>): List<String> =
        apps.map { "127.0.0.1:${it.getBoundPort()}" }

    private fun updatePeersAddresses(startedApps: List<Application>, notStartedApps: Int = 0) {
        val notStartedAppsAddresses = (1..notStartedApps)
            .map { "127.0.0.1:${Random.nextInt(5000, 10000)}" }

        val peers = getPeerAddresses(startedApps) + notStartedAppsAddresses

        startedApps.forEach { application ->
            val filteredPeers: List<String> = peers.filter { it != "127.0.0.1:${application.getBoundPort()}" }
            application.setOtherPeers(listOf(filteredPeers))
        }
    }

    private fun getPortFromAddress(address: String) = address.split(":")[1]

    private fun modifyPeers(app: Application, peers: List<String>) {
        val peers = peers.map { it.replace("http://", "") }
        app.setOtherPeers(listOf(peers))
    }


}
