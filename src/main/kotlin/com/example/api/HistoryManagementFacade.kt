package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.HistoryManagement

interface HistoryManagementFacade {
    fun change(changeDto: ChangeDto)
}

class HistoryManagementFacadeImpl(
    private val historyManagement: HistoryManagement
) : HistoryManagementFacade {
    override fun change(changeDto: ChangeDto) {
        historyManagement.change(changeDto.toChange())
    }
}