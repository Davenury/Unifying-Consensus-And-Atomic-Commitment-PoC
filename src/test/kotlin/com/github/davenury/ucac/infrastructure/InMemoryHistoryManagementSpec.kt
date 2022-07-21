package com.github.davenury.ucac.infrastructure

import com.github.davenury.ucac.common.AddRelationChange
import com.github.davenury.ucac.common.InMemoryHistoryManagement
import com.github.davenury.ucac.consensus.raft.domain.ConsensusFailure
import com.github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.utils.DummyConsensusProtocol
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
        val change = AddRelationChange("from", "to")
        // and - consensus protocol that's ok with changes
        consensusProtocol.setResponse(ConsensusSuccess)
        // when - change is proposed
        runBlocking {
            subject.change(change, 1)
        }// then - change should be done
        expectThat(subject.getLastChange()).isEqualTo(ChangeWithAcceptNum(change, 1))
    }

    @Test
    fun `should not add change if consensus protocol isn't ok with this`() {
        // given - some change
        val change = AddRelationChange("from", "to")
        // and - consensus protocol that isn't ok with changes
        consensusProtocol.setResponse(ConsensusFailure)

        // when - change is proposed
        runBlocking {
            subject.change(change, 1)
        }
        // then - change should not be added
        expectThat(subject.getLastChange()).isNull()
    }

    private val consensusProtocol = DummyConsensusProtocol
    private lateinit var subject: InMemoryHistoryManagement

}