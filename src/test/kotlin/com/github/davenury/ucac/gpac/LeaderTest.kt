package com.github.davenury.ucac.gpac

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.GPACProtocolClientImpl
import com.github.davenury.ucac.commitment.gpac.GPACProtocolImpl
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.utils.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
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

    private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

    private lateinit var history: History
    private lateinit var phaser: Phaser

    private lateinit var peerTwo: PeerWiremock
    private lateinit var peerThree: PeerWiremock

    private lateinit var subject: GPACProtocolImpl

    @BeforeEach
    fun setUp() {
        history = InMemoryHistory()
        phaser = Phaser(1).also { it.register() }

        peerTwo = PeerWiremock()
        peerThree = PeerWiremock()

        val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
        subject = GPACProtocolImpl(
            history,
            GpacConfig(3),
            ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
            GPACProtocolClientImpl(),
            TransactionBlocker(),
            peerResolver = PeerResolver(
                GlobalPeerId(1, 0),
                mapOf(
                    GlobalPeerId(1, 0) to PeerAddress(GlobalPeerId(1, 0), "localhost:8081"),
                    GlobalPeerId(1, 1) to PeerAddress(GlobalPeerId(1, 1), "localhost:${peerTwo.getPort()}"),
                    GlobalPeerId(1, 2) to PeerAddress(GlobalPeerId(1, 2), "localhost:${peerThree.getPort()}"),
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
    }

    @AfterEach
    fun tearDown() {
        peerTwo.close()
        peerThree.close()
    }

    @Test
    fun `should throw max retires exceeded, when too many times tried to be a leader`() {
        peerTwo.stubForNotElectingYou()
        peerThree.stubForNotElectingYou()

        runBlocking { subject.performProtocolAsLeader(change) }


        runBlocking {
            phaser.arriveAndAwaitAdvanceWithTimeout()
        }

        // assert that we're actually asking 3 times
        peerTwo.verifyMaxRetriesForElectionPassed(3)
        peerThree.verifyMaxRetriesForElectionPassed(3)
    }

    @Test
    fun `should throw TooFewResponsesException when not enough responses for agree message`() = runBlocking {
        peerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        peerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        peerTwo.stubForNotAgree()
        peerThree.stubForNotAgree()

        val result = subject.proposeChangeAsync(change).await()
        expectThat(result.detailedMessage).isNotNull()
        expectThat(result.detailedMessage).isEqualTo("Transaction failed due to too few responses of ft phase.")
    }

    @Test
    fun `should perform operation to the end`(): Unit = runBlocking {
        peerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        peerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        peerTwo.stubForAgree(10, Accept.COMMIT)
        peerThree.stubForAgree(10, Accept.COMMIT)
        peerTwo.stubForApply()
        peerThree.stubForApply()

        runBlocking { subject.performProtocolAsLeader(change) }

        expectThat(history.getCurrentEntry().let { Change.fromHistoryEntry(it) })
            .isEqualTo(change)
    }

    private val change = AddUserChange(
        "userName",
        peersets = listOf(
            ChangePeersetInfo(0, InitialHistoryEntry.getId()),
            ChangePeersetInfo(1, InitialHistoryEntry.getId()),
        ),
    )
}
