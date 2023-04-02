package com.github.davenury.ucac.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.txblocker.TransactionAcquisition
import com.github.davenury.common.txblocker.TransactionBlocker
import com.github.davenury.common.history.PersistentHistory
import com.github.davenury.common.persistence.InMemoryPersistence
import com.github.davenury.ucac.GpacConfig
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
import java.util.concurrent.Executors

@ExtendWith(TestLogExtension::class)
class GPACProtocolSpec {

    private val history = PersistentHistory(InMemoryPersistence())
    private val timerMock = mockk<ProtocolTimer>()
    private val protocolClientMock = mockk<GPACProtocolClient>()
    private val transactionBlockerMock = mockk<TransactionBlocker>()
    private var subject = GPACProtocolImpl(
        PeersetId("peerset0"),
        history,
        GpacConfig(3),
        ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
        protocolClientMock,
        transactionBlockerMock,
        peerResolver = PeerResolver(
            PeerId("peer0"),
            mapOf(
                PeerId("peer0") to PeerAddress(PeerId("peer1"), "peer1"),
                PeerId("peer1") to PeerAddress(PeerId("peer2"), "peer2"),
                PeerId("peer2") to PeerAddress(PeerId("peer3"), "peer3"),
            ),
            mapOf(
                PeersetId("peerset0") to listOf(PeerId("peer0"), PeerId("peer1"), PeerId("peer2")),
            )
        ),
    ).also {
        it.leaderTimer = timerMock
        it.retriesTimer = timerMock
    }

    @Test
    fun `should return elected you, when ballot number is lower than proposed`(): Unit = runBlocking {
        every { transactionBlockerMock.getAcquisition() } returns null
        every { runBlocking {  transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs
        every { runBlocking { transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) } } just Runs

        val message = ElectMe(100000, change)

        val result = subject.handleElect(message)

        expect {
            that(result).isEqualTo(ElectedYou(100000, Accept.COMMIT, 0, null, false))
            that(subject.getBallotNumber()).isEqualTo(100000)
        }
    }

    @Test
    fun `should throw NotElectingYou when ballot number is higher than proposed`(): Unit = runBlocking {
        every { transactionBlockerMock.getAcquisition() } returns null
        every { runBlocking {  transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs
        every { runBlocking {  transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs

        // -1 is not possible value according to protocol, but extending protocol class
        // with functionality of changing state is not the way
        val message = ElectMe(-1, change)

        expectThrows<NotElectingYou> {
            subject.handleElect(message)
        }
    }

    @Test
    fun `should return elected you with commit init val, when history can be built`(): Unit = runBlocking {
        every { transactionBlockerMock.getAcquisition() } returns null
        every { runBlocking {  transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs
        every { runBlocking { transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs

        val message = ElectMe(3, change)

        val result = subject.handleElect(message)

        expectThat(result).isEqualTo(ElectedYou(3, Accept.COMMIT, 0, null, false))
        expectThat(subject.getBallotNumber()).isEqualTo(3)
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`(): Unit = runBlocking {
        every { transactionBlockerMock.getAcquisition() } returns null
        every { runBlocking { transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs
        every { runBlocking {  transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) }} just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(100, change))
        val message = Agree(100, Accept.COMMIT, change)

        val result = subject.handleAgree(message)

        expectThat(result).isEqualTo(Agreed(100, Accept.COMMIT))
        expectThat(subject.getTransaction().ballotNumber).isEqualTo(100)
        expectThat(subject.getBallotNumber()).isEqualTo(100)
    }

    @Test
    fun `should throw not electing you, when proposed ballot number is less than state's`(): Unit = runBlocking {
        val message = Agree(-1, Accept.COMMIT, change)
        expectThrows<NotValidLeader> {
            subject.handleAgree(message)
        }
    }

    @Test
    fun `should apply change`(): Unit = runBlocking {
        every { transactionBlockerMock.getAcquisition() } returns null
        every { transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) } just Runs
        every { transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, change))
        subject.handleAgree(Agree(10, Accept.COMMIT, change))
        val message = Apply(10, true, Accept.COMMIT, change)

        subject.handleApply(message)
        expectThat(history.getCurrentEntry()).isEqualTo(change.toHistoryEntry(PeersetId("peerset0")))
    }

    @Test
    fun `should not apply change when acceptVal is abort`(): Unit = runBlocking {

        every { transactionBlockerMock.getAcquisition() } returns null
        every { transactionBlockerMock.acquireReentrant(TransactionAcquisition(ProtocolName.GPAC, change.id)) } just Runs
        every { transactionBlockerMock.tryRelease(TransactionAcquisition(ProtocolName.GPAC, change.id)) } returns true
        every { transactionBlockerMock.release(TransactionAcquisition(ProtocolName.GPAC, change.id)) } just Runs
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
            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
            ChangePeersetInfo(PeersetId("peerset1"), InitialHistoryEntry.getId()),
        ),
    )
}

