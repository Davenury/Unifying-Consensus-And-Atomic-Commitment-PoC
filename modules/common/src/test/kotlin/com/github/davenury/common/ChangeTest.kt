package com.github.davenury.common

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isNotEqualTo

/**
 * @author Kamil Jarosz
 */
internal class ChangeTest {
    @Test
    internal fun idUniqueness() {
        val change1 = AddUserChange(
            listOf(),
            "test1",
        )
        val change2 = AddUserChange(
            listOf(),
            "test2",
        )
        val change3 = change1.copyWithNewParentId(0, "")

        expectThat(change1.id).isNotEqualTo(change2.id)
        expectThat(change1.id).isNotEqualTo(change3.id)
    }
}
