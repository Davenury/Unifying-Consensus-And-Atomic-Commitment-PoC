package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.common.ChangeDto
import com.github.davenury.ucac.common.ErrorMessage
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.common.HistoryManagement
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.ratisRouting(historyManagement: HistoryManagement) {

    // Starting point for a Ktor app:
    routing {
        route("/change") {
            get {
                val result = historyManagement.getLastChange()
                call.respond((result?.toDto() ?: ErrorMessage("Error")))
            }
        }
        route("/changes") {
            get {
                val result = historyManagement.getState()
                call.respond((result?.toDto() ?: ErrorMessage("Error")))
            }
        }
    }
}

data class HistoryDto(
    val changes: List<ChangeWithAcceptNumDto>
)

data class ChangeWithAcceptNumDto(
    val change: ChangeDto,
    val acceptNum: Int?
)

fun ChangeWithAcceptNum.toDto() =
    ChangeWithAcceptNumDto(this.change.toDto(), acceptNum)

fun History.toDto() =
    HistoryDto(changes = this.map { it.toDto() })