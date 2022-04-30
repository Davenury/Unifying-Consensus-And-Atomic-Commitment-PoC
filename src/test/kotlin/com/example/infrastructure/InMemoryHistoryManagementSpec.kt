package com.example.infrastructure

import com.example.domain.AddRelation
import com.example.domain.ConsensusFailure
import com.example.domain.ConsensusSuccess
import com.example.domain.HistoryManagement
import com.example.utils.DummyConsensusProtocol
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

class InMemoryHistoryManagementSpec {

    @BeforeEach
    fun setup() {
        subject = InMemoryHistoryManagement(consensusProtocol)
    }

    @Test
    fun `should be able to add change, when consensus protocol is ok with it`() {
        // given - some change
        val change = AddRelation("from", "to")
        // and - consensus protocol that's ok with changes
        consensusProtocol.setResponse(ConsensusSuccess)
        // when - change is proposed
        subject.change(change)
        // then - change should be done
        expectThat(subject.getLastChange()).isEqualTo(change)
    }

    @Test
    fun `should not add change if consensus protocol isn't ok with this`() {
        // given - some change
        val change = AddRelation("from", "to")
        // and - consensus protocol that isn't ok with changes
        consensusProtocol.setResponse(ConsensusFailure)

        // when - change is proposed
        subject.change(change)

        // then - change should not be added
        expectThat(subject.getLastChange()).isNull()
    }

    private val consensusProtocol = DummyConsensusProtocol
    private lateinit var subject: HistoryManagement

}