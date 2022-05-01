package com.example.domain

import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

class OperationSpec {

    @Test
    fun `should be able to make add relation operation`() {
        val changeDto = ChangeDto(mapOf("operation" to "ADD_RELATION", "to" to "to", "from" to "from"))
        val change = changeDto.toChange()

        expectThat(change.operation).isEqualTo(Operation.ADD_RELATION)
    }

    @Test
    fun `should be able to make delete relation operation`() {
        val changeDto = ChangeDto(mapOf("operation" to "DELETE_RELATION", "to" to "to", "from" to "from"))
        val change = changeDto.toChange()

        expectThat(change.operation).isEqualTo(Operation.DELETE_RELATION)
    }

    @Test
    fun `should be able to make add user operation`() {
        val changeDto = ChangeDto(mapOf("operation" to "ADD_USER", "userName" to "userName"))
        val change = changeDto.toChange()

        expectThat(change.operation).isEqualTo(Operation.ADD_USER)
    }

    @Test
    fun `should be able to make add group operation`() {
        val changeDto = ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "groupName"))
        val change = changeDto.toChange()

        expectThat(change.operation).isEqualTo(Operation.ADD_GROUP)
    }

    @Test
    fun `should throw unknown operation exception, when operation is not known`() {
        val changeDto = ChangeDto(mapOf("operation" to "SOME_UNKNOWN_OPERATION", "to" to "to", "from" to "from"))

        expectThrows<UnknownOperationException> {
            changeDto.toChange()
        }
    }

    @Test
    fun `should throw missing parameter exception, when that's the case`() {
        val changeDto = ChangeDto(mapOf("operation" to "ADD_RELATION"))

        expectThrows<MissingParameterException> {
            changeDto.toChange()
        }
    }

    @Test
    fun `should not throw missing parameter exception, when there's some garbage in message`() {
        val changeDto = ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "groupName", "some_garbage" to "gfasdds"))

        expectCatching {
            changeDto.toChange()
        }.isSuccess()
    }

}