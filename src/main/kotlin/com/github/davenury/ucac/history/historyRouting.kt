package com.github.davenury.ucac.history

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.historyRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.history(): History {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).history
    }

    routing {
        route("/change") {
            get {
                call.history().getCurrentEntry()
                    .takeIf { it != InitialHistoryEntry }
                    ?.let { Change.fromHistoryEntry(it) }
                    ?.let { call.respond(it) }
                    ?: call.respond(
                        HttpStatusCode.NotFound,
                        ErrorMessage("No change exists")
                    )
            }
        }
        route("/changes") {
            get {
                val changes = Changes.fromHistory(call.history())
                log.info("Changes: $changes")
                call.respond(changes)
            }
        }
    }
}
