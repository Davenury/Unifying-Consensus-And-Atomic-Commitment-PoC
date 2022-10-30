package com.github.davenury.ucac.domain

import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.InMemoryHistoryManagement
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.common.TooFewResponsesException
import com.github.davenury.ucac.utils.DummyConsensusProtocol2
import com.github.davenury.ucac.gpac.domain.Accept
import com.github.davenury.ucac.gpac.domain.GPACProtocolClientImpl
import com.github.davenury.ucac.gpac.domain.GPACProtocolImpl
import com.github.davenury.ucac.gpac.domain.TransactionBlockerImpl
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.utils.PeerThree
import com.github.davenury.ucac.utils.PeerTwo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit

class LeaderTest {

    val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

    @Test
    fun `should throw max retires exceeded, when too many times tried to be a leader`() {
        PeerTwo.stubForNotElectingYou()
        PeerThree.stubForNotElectingYou()

        runBlocking { subject.performProtocolAsLeader(changeDto) }


        runBlocking {
            withContext(Dispatchers.IO) {
                phaser.awaitAdvanceInterruptibly(phaser.arrive(), 30, TimeUnit.SECONDS)
            }
        }

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

        expectThrows<TooFewResponsesException> { subject.performProtocolAsLeader(changeDto) }

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

        runBlocking { subject.performProtocolAsLeader(changeDto) }

        expectThat(historyManagement.getLastChange())
            .isEqualTo(changeDto)
    }

    private val allPeers = listOf(listOf("localhost:9091", "localhost:9092", "localhost:9093"))
    private val otherPeers = listOf(listOf("localhost:9092", "localhost:9093"))
    private val consensusProtocol = DummyConsensusProtocol2()
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol)
    private val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
    private val client = GPACProtocolClientImpl()
    private val transactionBlocker = TransactionBlockerImpl()
    private val phaser: Phaser = Phaser(1).also { it.register() }
    private val peerReachedMaxRetries = SignalListener {
        phaser.arrive()
    }

    val signalListenersForCohort = mapOf(
        Signal.ReachedMaxRetries to peerReachedMaxRetries
    )
    private var subject =
        GPACProtocolImpl(
            historyManagement,
            3,
            timer,
            client,
            transactionBlocker,
            myPeersetId = 1,
            myNodeId = 0,
            allPeers = mapOf(1 to listOf("localhost:${PeerTwo.getPort()}", "localhost:${PeerThree.getPort()}")),
            myAddress = "localhost:8081",
            signalPublisher = SignalPublisher(signalListenersForCohort)
        )
    private val changeDto = AddUserChange(InitialHistoryEntry.getId(), "userName", listOf())
}
