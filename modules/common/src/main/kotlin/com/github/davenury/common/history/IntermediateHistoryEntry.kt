package com.github.davenury.common.history

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.davenury.common.sha512

/**
 * Intermediate history entry represents a history entry that has a parent.
 *
 * @param content entry content in JSON format
 * @param parentId ID of the parent history entry which this entry is based on
 * @author Kamil Jarosz
 */
data class IntermediateHistoryEntry(
    private val content: String,
    private val parentId: String,
) : HistoryEntry {
    override fun getId(): String = sha512(serialize())

    override fun getParentId(): String = parentId

    override fun getContent(): String = content

    override fun serialize(): String {
        return ObjectMapper().writeValueAsString(
            mapOf(
                "parentId" to parentId,
                "content" to content,
            )
        )
    }
}
