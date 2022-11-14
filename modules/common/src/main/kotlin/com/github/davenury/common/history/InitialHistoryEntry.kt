package com.github.davenury.common.history

import com.github.davenury.common.sha512

object InitialHistoryEntry : HistoryEntry {
    override fun getId(): String = sha512(serialize())
    override fun getParentId(): String? = null
    override fun getContent(): String = ""
    override fun serialize(): String = "{}"
}
