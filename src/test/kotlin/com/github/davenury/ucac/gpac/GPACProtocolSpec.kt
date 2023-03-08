package com.github.davenury.ucac.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.InMemoryHistory
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.ResponsesTimeoutsConfig
import com.github.davenury.ucac.commitment.gpac.*
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.utils.TestLogExtension
import io.mockk.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import java.util.concurrent.Executors

@ExtendWith(TestLogExtension::class)
class GPACProtocolSpec {

    private val history = InMemoryHistory()
    private val timerMock = mockk<ProtocolTimer>()
    private val protocolClientMock = mockk<GPACProtocolClient>()
    private val transactionBlockerMock = mockk<TransactionBlocker>()
    private var subject = GPACProtocolImpl(
        history,
        GpacConfig(3),
        ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
        protocolClientMock,
        transactionBlockerMock,
        peerResolver = PeerResolver(
            GlobalPeerId(0, 0), mapOf(
                GlobalPeerId(0, 0) to PeerAddress(GlobalPeerId(0, 0), "peer1"),
                GlobalPeerId(0, 1) to PeerAddress(GlobalPeerId(0, 1), "peer2"),
                GlobalPeerId(0, 2) to PeerAddress(GlobalPeerId(0, 2), "peer3"),
            )
        ),
        isMetricTest = false,
        gpacResponsesContainer = GPACResponsesContainer(ResponsesTimeoutsConfig.default())
    ).also {
        it.leaderTimer = timerMock
        it.retriesTimer = timerMock
    }

    @Test
    fun `should return elected you, when ballot number is lower than proposed`(): Unit = runBlocking {

        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(100000, change)

        val result = subject.handleElect(message)

        expect {
            that(result).isEqualTo(ElectedYou(change = change, 100000, Accept.COMMIT, 0, null, false, elected = true, sender = GlobalPeerId(0, 0)))
            that(subject.getBallotNumber()).isEqualTo(100000)
        }
    }

    @Test
    fun `should throw NotElectingYou when ballot number is higher than proposed`(): Unit = runBlocking {

        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        // -1 is not possible value according to protocol, but extending protocol class
        // with functionality of changing state is not the way
        val message = ElectMe(-1, change)

        val result = subject.handleElect(message)
        expect {
            that(result.elected).isFalse()
            that(result.reason).isEqualTo(Reason.WRONG_BALLOT_NUMBER)
        }
    }

    @Test
    fun `should return elected you with commit init val, when history can be built`(): Unit = runBlocking {

        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(3, change)

        val result = subject.handleElect(message)

        expectThat(result).isEqualTo(ElectedYou(change, 3, Accept.COMMIT, 0, null, false, elected = true, sender = GlobalPeerId(0, 0)))
        expectThat(subject.getBallotNumber()).isEqualTo(3)
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`(): Unit = runBlocking {

        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(100, change))
        val message = Agree(100, Accept.COMMIT, change)

        val result = subject.handleAgree(message)

        expectThat(result).isEqualTo(Agreed(change, 100, Accept.COMMIT, agreed = true, sender = GlobalPeerId(0, 0)))
        expectThat(subject.getTransaction().ballotNumber).isEqualTo(100)
        expectThat(subject.getBallotNumber()).isEqualTo(100)
    }

    @Test
    fun `should throw not electing you, when proposed ballot number is less than state's`(): Unit = runBlocking {
        val message = Agree(-1, Accept.COMMIT, change)
        val result = subject.handleAgree(message)
        expect {
            that(result.agreed).isFalse()
            that(result.reason).isEqualTo(Reason.NOT_VALID_LEADER)
        }
    }

    @Test
    fun `should apply change`(): Unit = runBlocking {
        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        every { transactionBlockerMock.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id) } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, change))
        subject.handleAgree(Agree(10, Accept.COMMIT, change))
        val message = Apply(10, true, Accept.COMMIT, change)

        subject.handleApply(message)
        expectThat(history.getCurrentEntry()).isEqualTo(change.toHistoryEntry(0))
    }

    @Test
    fun `should not apply change when acceptVal is abort`(): Unit = runBlocking {

        every { transactionBlockerMock.isAcquired() } returns false
        every { transactionBlockerMock.tryToBlock(ProtocolName.GPAC, change.id) } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        every { transactionBlockerMock.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id) } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, change))
        subject.handleAgree(Agree(10, Accept.ABORT, change))
        val message = Apply(10, true, Accept.ABORT, change)

        subject.handleApply(message)
        expectThat(history.getCurrentEntry()).isEqualTo(InitialHistoryEntry)
    }

    private val change = AddUserChange(
        "userName",
        peersets = listOf(
            ChangePeersetInfo(0, InitialHistoryEntry.getId()),
            ChangePeersetInfo(1, InitialHistoryEntry.getId()),
        ),
    )
}
