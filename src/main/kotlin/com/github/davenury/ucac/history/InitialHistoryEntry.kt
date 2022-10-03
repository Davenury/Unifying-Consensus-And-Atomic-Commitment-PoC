package com.github.davenury.ucac.history

import com.github.davenury.ucac.sha512

object InitialHistoryEntry : HistoryEntry {
    override fun getId(): String = sha512(serialize())
    override fun getParentId(): String? = null
    override fun getContent(): String = ""
    override fun serialize(): String = "{}"
}
