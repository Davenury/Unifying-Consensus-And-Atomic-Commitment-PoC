package com.github.davenury.ucac.domain

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.raft.infrastructure.DummyConsensusProtocol
import com.github.davenury.ucac.gpac.domain.Accept
import com.github.davenury.ucac.gpac.domain.GPACProtocolClientImpl
import com.github.davenury.ucac.gpac.domain.GPACProtocolImpl
import com.github.davenury.ucac.gpac.domain.TransactionBlockerImpl
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.utils.PeerThree
import com.github.davenury.ucac.utils.PeerTwo
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import java.time.Duration
import java.util.concurrent.Executors

class LeaderTest {

    val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

    @Test
    fun `should throw max retires exceeded, when too many times tried to be a leader`() {
        PeerTwo.stubForNotElectingYou()
        PeerThree.stubForNotElectingYou()

        expectThrows<MaxTriesExceededException> { subject.performProtocolAsLeader(change) }

        // assert that we're actually asking 3 times
        PeerTwo.verifyMaxRetriesForElectionPassed(3)
        PeerThree.verifyMaxRetriesForElectionPassed(3)
    }

    @Test
    fun `should throw TooFewResponsesException when not enough responses for agree message`() {
        PeerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerTwo.stubForNotAgree()
        PeerThree.stubForNotAgree()

        expectThrows<TooFewResponsesException> { subject.performProtocolAsLeader(change) }

        PeerTwo.verifyAgreeStub(1)
        PeerThree.verifyAgreeStub(1)
    }

    @Test
    fun `should perform operation to the end`(): Unit = runBlocking {
        PeerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerTwo.stubForAgree(10, Accept.COMMIT)
        PeerThree.stubForAgree(10, Accept.COMMIT)
        PeerTwo.stubForApply()
        PeerThree.stubForApply()

        runBlocking {
            subject.performProtocolAsLeader(change)
        }

        expectThat(Change.fromHistoryEntry(historyManagement.getLastChange()))
            .isEqualTo(change)
    }

    private val allPeers = listOf(listOf("localhost:9091", "localhost:9092", "localhost:9093"))
    private val otherPeers = listOf(listOf("localhost:9092", "localhost:9093"))
    private val history = History()
    private val consensusProtocol = DummyConsensusProtocol(history)
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol, history)
    private val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
    private val client = GPACProtocolClientImpl()
    private val transactionBlocker = TransactionBlockerImpl()
    private var subject =
        GPACProtocolImpl(
            historyManagement,
            3,
            timer,
            client,
            transactionBlocker,
            myPeersetId = 1,
            myNodeId = 0,
            allPeers = mapOf(1 to listOf("localhost:9092", "localhost:9093")),
            myAddress = "localhost:8081"
        )
    private val change = AddUserChange(InitialHistoryEntry.getId(), "userName", listOf())
}
