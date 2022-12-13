package com.github.davenury.ucac.gpac

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Transition
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.GPACProtocolClientImpl
import com.github.davenury.ucac.commitment.gpac.GPACProtocolImpl
import com.github.davenury.ucac.common.*
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
            phaser.arriveAndAwaitAdvanceWithTimeout()
        }

        // assert that we're actually asking 3 times
        PeerTwo.verifyMaxRetriesForElectionPassed(3)
        PeerThree.verifyMaxRetriesForElectionPassed(3)
    }

    @Test
    fun `should throw TooFewResponsesException when not enough responses for agree message`() = runBlocking {
        PeerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerTwo.stubForNotAgree()
        PeerThree.stubForNotAgree()

        val result = subject.proposeChangeAsync(change).await()
        expectThat(result.detailedMessage).isNotNull()
        expectThat(result.detailedMessage).isEqualTo("Transaction failed due to too few responses of ft phase.")

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

        expectThat(history.getCurrentEntry().let { Transition.fromHistoryEntry(it)?.change })
            .isEqualTo(change)
    }

    private val history = InMemoryHistory()
    private val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
    private val client = GPACProtocolClientImpl()
    private val transactionBlocker = TransactionBlocker()
    private val phaser: Phaser = Phaser(1).also { it.register() }

    private var subject = GPACProtocolImpl(
        history,
        GpacConfig(3),
        ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
        client,
        transactionBlocker,
        peerResolver = PeerResolver(
            GlobalPeerId(1, 0),
            mapOf(
                GlobalPeerId(1, 0) to PeerAddress(GlobalPeerId(1, 0), "localhost:8081"),
                GlobalPeerId(1, 1) to PeerAddress(GlobalPeerId(1, 1), "localhost:${PeerTwo.getPort()}"),
                GlobalPeerId(1, 2) to PeerAddress(GlobalPeerId(1, 2), "localhost:${PeerThree.getPort()}"),
            )
        ),
        signalPublisher = SignalPublisher(
            mapOf(
                Signal.ReachedMaxRetries to SignalListener {
                    phaser.arrive()
                }
            )
        ),
        isMetricTest = false
    ).also {
        it.leaderTimer = timer
        it.retriesTimer = timer
    }

    private val change = AddUserChange(
        "userName",
        peersets = listOf(
            ChangePeersetInfo(0, InitialHistoryEntry.getId()),
            ChangePeersetInfo(1, InitialHistoryEntry.getId()),
        ),
    )
}
