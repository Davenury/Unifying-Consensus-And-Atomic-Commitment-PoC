package com.example.api

import com.example.domain.Change
import com.example.domain.HistoryManagement
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json

interface HistoryManagementFacade {
    fun change(change: String)
}

class HistoryManagementFacadeImpl(
    private val historyManagement: HistoryManagement
): HistoryManagementFacade {
    override fun change(change: String) {
        Json.decodeFromString<Change>(change).let {
            historyManagement.change(it)
        }
    }
}