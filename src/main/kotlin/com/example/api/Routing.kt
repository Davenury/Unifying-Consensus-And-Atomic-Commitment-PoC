package com.example.api

import com.example.domain.ChangeDto
import com.example.domain.ErrorMessage
import com.example.domain.History
import com.example.domain.HistoryManagement
import com.example.ratis.ChangeWithAcceptNum
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.configureSampleRouting(historyManagement: HistoryManagement) {

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