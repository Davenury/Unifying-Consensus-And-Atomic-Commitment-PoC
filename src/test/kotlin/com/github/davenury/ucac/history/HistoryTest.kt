package com.github.davenury.ucac.history

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThrows
import java.lang.IllegalArgumentException

/**
 * @author Kamil Jarosz
 */
internal class HistoryTest {
    @Test
    fun `initial entry`(): Unit = runBlocking {
        val history = History()
        assert(history.getCurrentEntry() == InitialHistoryEntry)
    }

    @Test
    fun `add entry`(): Unit = runBlocking {
        val history = History()

        val parentId = history.getCurrentEntry().getId()
        val entry = IntermediateHistoryEntry("test", parentId)
        history.addEntry(entry)

        assert(history.getCurrentEntry() == entry)
    }

    @Test
    fun `add wrong entry`(): Unit = runBlocking {
        val history = History()

        val entry = IntermediateHistoryEntry("test", "non existent id")
        expectThrows<HistoryException> {
            history.addEntry(entry)
        }
    }

    @Test
    fun `add the same entry multiple times`(): Unit = runBlocking {
        val history = History()

        val parentId = history.getCurrentEntry().getId()
        val entry1 = IntermediateHistoryEntry("test1", parentId)
        val entry2 = IntermediateHistoryEntry("test2", parentId)
        history.addEntry(entry1)
        expectThrows<HistoryException> {
            history.addEntry(entry2)
        }
    }

    @Test
    fun `get entry from history`(): Unit = runBlocking {
        val history = History()

        val parentId = history.getCurrentEntry().getId()
        val entry1 = IntermediateHistoryEntry("test1", parentId)
        val entry2 = IntermediateHistoryEntry("test2", entry1.getId())
        val entry3 = IntermediateHistoryEntry("test2", entry2.getId())
        history.addEntry(entry1)
        history.addEntry(entry2)

        assert(history.getEntryFromHistory(InitialHistoryEntry.getId()) == InitialHistoryEntry)
        assert(history.getEntryFromHistory(entry1.getId()) == entry1)
        assert(history.getEntryFromHistory(entry2.getId()) == entry2)
        assert(history.getEntryFromHistory(entry3.getId()) == null)
    }

    @Test
    fun `serialize entry`(): Unit = runBlocking {
        val entry = IntermediateHistoryEntry("test1", "parent")

        assert(entry.serialize() == """{"content":"test1","parentId":"parent"}""")
    }

    @Test
    fun `serialize initial entry`(): Unit = runBlocking {
        val entry = InitialHistoryEntry

        assert(entry.serialize() == "{}")
    }

    @Test
    fun `deserialize entry`(): Unit = runBlocking {
        val entry = IntermediateHistoryEntry("test1", "parent")
        val deserialized = HistoryEntry.deserialize(entry.serialize())

        assert(entry == deserialized)
        assert(deserialized.getContent() == "test1")
        assert(deserialized.getParentId() == "parent")
    }

    @Test
    fun `deserialize entry with wrong formatting`(): Unit = runBlocking {
        expectThrows<IllegalArgumentException> {
            HistoryEntry.deserialize("{ }")
        }
    }

    @Test
    fun `deserialize entry with wrong order`(): Unit = runBlocking {
        expectThrows<IllegalArgumentException> {
            HistoryEntry.deserialize("""{"parentId":"parent","content":"content"}""")
        }
        HistoryEntry.deserialize("""{"content":"content","parentId":"parent"}""")
    }

    @Test
    fun `id generation`(): Unit = runBlocking {
        assert(InitialHistoryEntry.getId() == "27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
    }
}
