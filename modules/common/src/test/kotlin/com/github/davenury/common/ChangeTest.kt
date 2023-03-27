package com.github.davenury.common

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo

/**
 * @author Kamil Jarosz
 */
internal class ChangeTest {
    @Test
    internal fun idUniqueness() {
        val change1 = AddUserChange(
            "test1",
            peersets = listOf(),
        )
        val change2 = AddUserChange(
            "test2",
            peersets = listOf(),
        )
        val change3 = change1.copyWithNewParentId(PeersetId("ps"), "")

        expectThat(change1.id).isNotEqualTo(change2.id)
        expectThat(change1.id).isEqualTo(change3.id)
    }
}
