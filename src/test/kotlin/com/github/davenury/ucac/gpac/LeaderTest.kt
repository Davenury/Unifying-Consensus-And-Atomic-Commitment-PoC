package com.github.davenury.ucac.gpac

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.TooFewResponsesException
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.GPACProtocolClientImpl
import com.github.davenury.ucac.commitment.gpac.GPACProtocolImpl
import com.github.davenury.ucac.commitment.gpac.TransactionBlockerImpl
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.utils.PeerThree
import com.github.davenury.ucac.utils.PeerTwo
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Phaser

@ExtendWith(TestLogExtension::class)
class LeaderTest {

    val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

    @Test
    fun `should throw max retires exceeded, when too many times tried to be a leader`() {
        PeerTwo.stubForNotElectingYou()
        PeerThree.stubForNotElectingYou()

        runBlocking { subject.performProtocolAsLeader(change) }


        runBlocking {
            phaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(15))
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

        expectThrows<TooFewResponsesException> {
            val result = subject.proposeChangeAsync(change).await()
            expectThat(result.exception).isNotNull()
            throw result.exception!!
        }
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

        runBlocking { subject.performProtocolAsLeader(change) }

        expectThat(history.getCurrentEntry().let { Change.fromHistoryEntry(it) })
            .isEqualTo(change)
    }

    private val history = History()
    private val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
    private val client = GPACProtocolClientImpl()
    private val transactionBlocker = TransactionBlockerImpl()
    private val phaser: Phaser = Phaser(1).also { it.register() }
    private val peerReachedMaxRetries = SignalListener {
        phaser.arrive()
    }

    private var subject =
        GPACProtocolImpl(
            history,
            GpacConfig(3),
            ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
            client,
            transactionBlocker,
            myPeersetId = 1,
            myNodeId = 0,
            allPeers = mapOf(
                1 to listOf(
                    "localhost:8081",
                    "localhost:${PeerTwo.getPort()}",
                    "localhost:${PeerThree.getPort()}"
                )
            ),
            myAddress = "localhost:8081",
            signalPublisher = SignalPublisher(
                mapOf(
                    Signal.ReachedMaxRetries to peerReachedMaxRetries
                )
            )
        ).also {
            it.leaderTimer = timer
            it.retriesTimer = timer
        }
    private val change = AddUserChange(InitialHistoryEntry.getId(), "userName", listOf("localhost:8081"))
}
