package com.example.api

import com.example.domain.*
import com.example.infrastructure.InMemoryHistoryManagement
import com.example.utils.DummyConsensusProtocol
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class HistoryManagementFacadeSpec {

    @Test
    fun `should be able to add history change`() {
        // given - change
        val change = "{\"to\":\"to\",\"from\":\"from\",\"operation\":\"ADD_RELATION\"}"

        // when - proposing change
        subject.change(change)
        // then - change should be added to storage
        expectThat(storage.getLastChange()).isEqualTo(Change("from", "to", "ADD_RELATION"))
    }

    private val consensusProtocol = DummyConsensusProtocol
    private val storage: HistoryManagement = InMemoryHistoryManagement(consensusProtocol)
    private val subject = HistoryManagementFacadeImpl(storage)
}
