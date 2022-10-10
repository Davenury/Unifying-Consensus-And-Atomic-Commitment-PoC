package com.github.davenury.ucac.common

import com.github.davenury.ucac.objectMapper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

/**
 * @author Kamil Jarosz
 */
internal class ChangeSpec {
    @Test
    fun `serialize add relation change`(): Unit = runBlocking {
        val change = AddRelationChange("prentId", "from", "to", listOf("p1"))
        val serialized = objectMapper.writeValueAsString(change)

        expectThat(serialized).isEqualTo("""{"@type":"ADD_RELATION","parentId":"prentId","from":"from","to":"to","peers":["p1"],"acceptNum":null}""")
    }

    @Test
    fun `deserialize add relation change`(): Unit = runBlocking {
        val serialized = """{"@type":"ADD_RELATION","parentId":"prentId","from":"from","to":"to","peers":["p1"],"acceptNum":null}"""
        val deserialized = Change.fromJson(serialized)

        expectThat(deserialized).isEqualTo(AddRelationChange("prentId", "from", "to", listOf("p1")))
    }
}
