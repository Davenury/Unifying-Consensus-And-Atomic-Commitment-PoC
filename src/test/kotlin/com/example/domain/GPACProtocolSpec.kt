package com.example.domain

import com.example.infrastructure.InMemoryHistoryManagement
import com.example.utils.DummyConsensusProtocol
import io.ktor.client.*
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class GPACProtocolSpec {

    private val consensusProtocol = DummyConsensusProtocol
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol)
    private val subject = GPACProtocolImpl(historyManagement, 3, HttpClient())

    @Test
    fun `should return elected you, when ballot number is lower than proposed`() {
        val message = ElectMe(100000, changeDto)

        val result = subject.handleElect(message)

        expect {
            // ballot number is unchanged for now
            that(result.ballotNumber).isEqualTo(0)
            that(subject.getState().ballotNumber).isEqualTo(0)
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
        val message = ElectMe(1, changeDto)

        val result = subject.handleElect(message)

        expectThat(result.initVal).isEqualTo(Accept.COMMIT)
        expectThat(subject.getState().ballotNumber).isEqualTo(0)
    }

    @Test
    fun `should change ballot number and return agreed, when asked to ft-agree on change`() {
        val message = Agree(100, Accept.COMMIT, changeDto)

        val result = subject.handleAgree(message)

        expectThat(result).isEqualTo(Agreed(100, Accept.COMMIT))
        expectThat(subject.getState().ballotNumber).isEqualTo(100)
    }

    @Test
    fun `should throw not electing you, when proposed ballot number is less than state's`() {
        val message = Agree(-1, Accept.COMMIT, changeDto)
        expectThrows<NotElectingYou> {
            subject.handleAgree(message)
        }
    }

    @Test
    fun `should apply change and state should reset after ending transaction`() {
        val message = Apply(10, true, changeDto)

        subject.handleApply(message)
        expectThat(historyManagement.getLastChange()).isEqualTo(AddUserChange("userName"))
        expectThat(subject.getState()).isEqualTo(State(
            // as 0 is previous state
            0,
            Accept.ABORT,
            0,
            null,
            false
        ))
    }

    private val changeDto = ChangeDto(mapOf(
        "operation" to "ADD_USER",
        "userName" to "userName"
    ))

}