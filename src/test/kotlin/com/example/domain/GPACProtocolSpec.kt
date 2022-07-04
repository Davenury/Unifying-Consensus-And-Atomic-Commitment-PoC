package com.example.domain

import com.example.infrastructure.InMemoryHistoryManagement
import com.example.raft.ChangeWithAcceptNum
import com.example.utils.DummyConsensusProtocol
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class GPACProtocolSpec {

    private val consensusProtocol = DummyConsensusProtocol
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol)
    private val timerMock = mockk<ProtocolTimer>()
    private val protocolClientMock = mockk<ProtocolClient>()
    private val transactionBlockerMock = mockk<TransactionBlocker>()
    private var subject = GPACProtocolImpl(
        historyManagement,
        3,
        timerMock,
        protocolClientMock,
        transactionBlockerMock,
        listOf(listOf("http://localhost:8080")),
        me = 8080
    )

    @BeforeEach
    fun setup() {
        subject = GPACProtocolImpl(
            historyManagement,
            3,
            timerMock,
            protocolClientMock,
            transactionBlockerMock,
            listOf(listOf("http://localhost:8080")),
            me = 8080
        )
    }

    @Test
    fun `should return elected you, when ballot number is lower than proposed`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(100000, changeDto)

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
        val message = ElectMe(-1, changeDto)

        expectThrows<NotElectingYou> {
            subject.handleElect(message)
        }
    }

    @Test
    fun `should return elected you with commit init val, when history can be built`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs

        val message = ElectMe(3, changeDto)

        val result = subject.handleElect(message)

        expectThat(result.initVal).isEqualTo(Accept.COMMIT)
        expectThat(subject.getTransaction()).isEqualTo(Transaction(3, Accept.COMMIT, 0, null, false))
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(100, changeDto))
        val message = Agree(100, Accept.COMMIT, changeDto)

        val result = subject.handleAgree(message)

        expectThat(result).isEqualTo(Agreed(100, Accept.COMMIT))
        expectThat(subject.getTransaction().ballotNumber).isEqualTo(100)
        expectThat(subject.getBallotNumber()).isEqualTo(100)
    }

    @Test
    fun `should throw not electing you, when proposed ballot number is less than state's`(): Unit = runBlocking {
        val message = Agree(-1, Accept.COMMIT, changeDto)
        expectThrows<NotValidLeader> {
            subject.handleAgree(message)
        }
    }

    @Test
    fun `should apply change`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, changeDto))
        subject.handleAgree(Agree(10, Accept.COMMIT, changeDto))
        val message = Apply(10, true, Accept.COMMIT, changeDto)

        subject.handleApply(message)
        expectThat(historyManagement.getLastChange()).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 10))
    }

    @Test
    fun `should not apply change when acceptVal is abort`(): Unit = runBlocking {

        every { transactionBlockerMock.assertICanSendElectedYou() } just Runs
        every { transactionBlockerMock.tryToBlock() } just Runs
        every { transactionBlockerMock.releaseBlock() } just Runs
        coEvery { timerMock.startCounting(any()) } just Runs
        every { timerMock.cancelCounting() } just Runs

        subject.handleElect(ElectMe(10, changeDto))
        subject.handleAgree(Agree(10, Accept.ABORT, changeDto))
        val message = Apply(10, true, Accept.ABORT, changeDto)

        subject.handleApply(message)
        expectThat(historyManagement.getLastChange()).isEqualTo(null)
    }

    private val changeDto = ChangeDto(
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )
    )

}