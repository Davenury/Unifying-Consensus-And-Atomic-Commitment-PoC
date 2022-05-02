package com.example.api

import com.example.domain.*
import com.example.infrastructure.InMemoryHistoryManagement
import com.example.utils.DummyConsensusProtocol
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class HistoryManagementFacadeSpec {

    @Test
    fun `should be able to add history change`() {
        // given - change
        val change = mapOf("operation" to "ADD_RELATION", "from" to "from", "to" to "to")

        // when - proposing change
        subject.change(ChangeDto(change))
        // then - change should be added to storage
        expectThat(storage.getLastChange()).isEqualTo(AddRelationChange("from", "to"))
    }

    @Test
    fun `should throw missing parameter exception, when operation value is missing`() {
        // given - change without operation
        val change = mapOf("from" to "from", "to" to "to")


        // when - proposing change; then - throw exception
        expectThrows<MissingParameterException> {
            subject.change(ChangeDto(change))
        }
    }

    private val consensusProtocol = DummyConsensusProtocol
    private val storage: HistoryManagement = InMemoryHistoryManagement(consensusProtocol)
    private val subject = HistoryManagementFacadeImpl(storage)
}
