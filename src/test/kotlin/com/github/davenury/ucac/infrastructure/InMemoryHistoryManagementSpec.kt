package com.github.davenury.ucac.infrastructure

import com.github.davenury.ucac.common.AddRelationChange
import com.github.davenury.ucac.common.InMemoryHistoryManagement
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusFailure
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.utils.DummyConsensusProtocol
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class InMemoryHistoryManagementSpec {

    private lateinit var consensusProtocol: DummyConsensusProtocol
    private lateinit var subject: InMemoryHistoryManagement

    @BeforeEach
    fun setup() {
        consensusProtocol = DummyConsensusProtocol()
        subject = InMemoryHistoryManagement(consensusProtocol, consensusProtocol.history)
    }

    @Test
    fun `should be able to add change, when consensus protocol is ok with it`() {
        // given - some change
        val change = AddRelationChange(InitialHistoryEntry.getId(), "from", "to", listOf(), 1)
        // and - consensus protocol that's ok with changes
        val historyEntry = change.toHistoryEntry()
        consensusProtocol.history.addEntry(historyEntry)
        consensusProtocol.setResponse(ConsensusSuccess)
        // when - change is proposed
        runBlocking {
            subject.change(historyEntry)
        }// then - change should be done
        expectThat(subject.getLastChange()).isEqualTo(historyEntry)
    }

    @Test
    fun `should not add change if consensus protocol isn't ok with this`() {
        // given - some change
        val change = AddRelationChange("parentId", "from", "to", listOf())
        // and - consensus protocol that isn't ok with changes
        consensusProtocol.setResponse(ConsensusFailure)

        // when - change is proposed
        runBlocking {
            subject.change(change.toHistoryEntry())
        }
        // then - change should not be added
        expectThat(subject.getLastChange()).isEqualTo(InitialHistoryEntry)
    }

}
