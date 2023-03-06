package com.github.davenury.common.history

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isFalse
import strikt.assertions.isTrue

/**
 * @author Kamil Jarosz
 */
internal class InMemoryHistoryTest {
    private lateinit var history: History
    private lateinit var entries: ArrayList<HistoryEntry>

    @BeforeEach
    fun setUp() {
        history = InMemoryHistory()
        entries = ArrayList()

        for (i in 1..10) {
            val entry = IntermediateHistoryEntry("test $i", history.getCurrentEntryId())
            history.addEntry(entry)
            entries.add(entry)
        }
    }

    @Test
    internal fun containsInitialEntry() {
        expectThat(history.containsEntry(InitialHistoryEntry.getId())).isTrue()
    }

    @Test
    internal fun contains1stEntry() {
        expectThat(history.containsEntry(entries[0].getId())).isTrue()
    }

    @Test
    internal fun contains6thEntry() {
        expectThat(history.containsEntry(entries[5].getId())).isTrue()
    }

    @Test
    internal fun containsLastEntry() {
        expectThat(history.containsEntry(entries[9].getId())).isTrue()
    }

    @Test
    internal fun containsCurrentEntry() {
        expectThat(history.containsEntry(history.getCurrentEntryId())).isTrue()
    }

    @Test
    internal fun containsEntryNonexistent() {
        expectThat(history.containsEntry("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")).isFalse()
    }

    @Test
    internal fun containsEntryInvalid() {
        expectThat(history.containsEntry("non-existent")).isFalse()
    }

    @Test
    internal fun doesNotContainOldAncestors() {
        history.containsEntry(InitialHistoryEntry.getId())
        history.addEntry(IntermediateHistoryEntry("next", history.getCurrentEntryId()))
        history.containsEntry(InitialHistoryEntry.getId())

        expectThat((history as CachedHistory).ancestors)
            .hasSize(1)
    }
}
