package com.github.davenury.ucac.gpac

import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.InitialHistoryEntry
import io.mockk.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import java.util.concurrent.Executors

class GPACProtocolSpec {

    private val history = History()
    private val timerMock = mockk<ProtocolTimer>()
    private val protocolClientMock = mockk<GPACProtocolClient>()
    private val transactionBlockerMock = mockk<TransactionBlocker>()
    private var subject = GPACProtocolImpl(
        history,
        GpacConfig(3),
        ctx = Executors.newCachedThreadPool().asCoroutineDispatcher(),
        protocolClientMock,
        transactionBlockerMock,
        myPeersetId = 0,
        myNodeId = 0,
        allPeers = mapOf(0 to listOf("peer2", "peer3")),
        myAddress = "peer1"
    ).also {
        it.leaderTimer = timerMock
        it.retriesTimer = timerMock
    }

    @Test
    fun `should return elected you, when ballot number is lower than proposed`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(100000, change)

        val result = subject.handleElect(message)

        expect {
            that(result).isEqualTo(ElectedYou(100000, Accept.COMMIT, 0, null, false))
            that(subject.getTransaction().ballotNumber).isEqualTo(100000)
        }
    }

    @Test
    fun `should throw NotElectingYou when ballot number is higher than proposed`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        // -1 is not possible value according to protocol, but extending protocol class
        // with functionality of changing state is not the way
        val message = ElectMe(-1, change)

        expectThrows<NotElectingYou> {
            subject.handleElect(message)
        }
    }

    @Test
    fun `should return elected you with commit init val, when history can be built`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(3, change)

        val result = subject.handleElect(message)

        expectThat(result.initVal).isEqualTo(Accept.COMMIT)
        expectThat(subject.getTransaction()).isEqualTo(Transaction(3, Accept.COMMIT, 0, null, false, change = change))
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
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

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, change))
        subject.handleAgree(Agree(10, Accept.COMMIT, change))
        val message = Apply(10, true, Accept.COMMIT, change)

        subject.handleApply(message)
        expectThat(history.getCurrentEntry()).isEqualTo(change.toHistoryEntry())
    }

    @Test
    fun `should not apply change when acceptVal is abort`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(action = any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, change))
        subject.handleAgree(Agree(10, Accept.ABORT, change))
        val message = Apply(10, true, Accept.ABORT, change)

        subject.handleApply(message)
        expectThat(history.getCurrentEntry()).isEqualTo(InitialHistoryEntry)
    }

    private val change = AddUserChange(
        InitialHistoryEntry.getId(),
        "userName",
        listOf("peer2"),
    )

}
