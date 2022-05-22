package com.example.domain

import com.example.infrastructure.InMemoryHistoryManagement
import com.example.utils.DummyConsensusProtocol
import io.ktor.client.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class GPACProtocolSpec {

    private val consensusProtocol = DummyConsensusProtocol
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol)
    private var subject = GPACProtocolImpl(historyManagement, 3, HttpClient())

    @BeforeEach
    fun setup() {
        subject = GPACProtocolImpl(historyManagement, 3, HttpClient())
    }

    @Test
    fun `should return elected you, when ballot number is lower than proposed`() {
        val message = ElectMe(100000, changeDto)

        val result = subject.handleElect(message)

        expect {
            that(result).isEqualTo(ElectedYou(100000, Accept.COMMIT, 0, null, false))
            that(subject.getTransaction(100000)!!.ballotNumber).isEqualTo(100000)
        }
    }

    @Test
    fun `should throw NotElectingYou when ballot number is higher than proposed`() {
        // -1 is not possible value according to protocol, but extending protocol class
        // with functionality of changing state is not the way
        val message = ElectMe(-1, changeDto)

        expectThrows<NotElectingYou> {
            subject.handleElect(message)
        }
    }

    @Test
    fun `should return elected you with commit init val, when history can be built`() {
        val message = ElectMe(3, changeDto)

        val result = subject.handleElect(message)

        expectThat(result.initVal).isEqualTo(Accept.COMMIT)
        expectThat(subject.getTransaction(3)).isEqualTo(Transaction(3, Accept.COMMIT, 0, null, false))
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`() {
        subject.handleElect(ElectMe(100, changeDto))
        val message = Agree(100, Accept.COMMIT, changeDto)

        val result = subject.handleAgree(message)

        expectThat(result).isEqualTo(Agreed(100, Accept.COMMIT))
        expectThat(subject.getTransaction(100)!!.ballotNumber).isEqualTo(100)
        expectThat(subject.getBallotNumber()).isEqualTo(100)
    }

    @Test
    fun `should throw IllegalState when trying to agree on transaction that wasn't in elect state`() {
        val message = Agree(100, Accept.COMMIT, changeDto)

        expectThrows<IllegalStateException> {
            subject.handleAgree(message)
        }
    }

    @Test
    fun `should throw not electing you, when proposed ballot number is less than state's`() {
        val message = Agree(-1, Accept.COMMIT, changeDto)
        expectThrows<NotElectingYou> {
            subject.handleAgree(message)
        }
    }

    @Test
    fun `should apply change`() {
        subject.handleElect(ElectMe(10, changeDto))
        subject.handleAgree(Agree(10, Accept.COMMIT, changeDto))
        val message = Apply(10, true, Accept.COMMIT, changeDto)

        subject.handleApply(message)
        expectThat(historyManagement.getLastChange()).isEqualTo(AddUserChange("userName"))
    }

    @Test
    fun `should not apply change when acceptVal is abort`() {
        subject.handleElect(ElectMe(10, changeDto))
        subject.handleAgree(Agree(10, Accept.ABORT, changeDto))
        val message = Apply(10, true, Accept.ABORT, changeDto)

        subject.handleApply(message)
        expectThat(historyManagement.getLastChange()).isEqualTo(null)
    }

    private val changeDto = ChangeDto(mapOf(
        "operation" to "ADD_USER",
        "userName" to "userName"
    ))

}