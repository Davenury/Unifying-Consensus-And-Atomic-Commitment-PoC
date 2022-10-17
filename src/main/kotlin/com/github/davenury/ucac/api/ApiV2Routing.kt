package com.github.davenury.ucac.api

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.common.HistoryManagement
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.history.History
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.concurrent.CompletableFuture

private const val DEFAULT_SYNC_TIMEOUT = "PT1M"

class ApiV2Routing(
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
    historyManagement: HistoryManagement,
    private val config: Config,
) {
    private val history: History = historyManagement.getState()

    fun addRoutingFor(app: Application) {
        app.commonRouting()
    }

    private suspend fun addChange(change: Change): CompletableFuture<ChangeResult> {
        val peers = change.peers
        return if (config.peerAddresses(config.peersetId).containsAll(peers)) {
            consensusProtocol.proposeChangeAsync(change)
        } else {
            gpacProtocol.proposeChangeAsync(change)
        }
    }

    private fun Application.commonRouting() {
        routing {
            post("/v2/change/async") {
                val change = call.receive<Change>()

                addChange(change)

                call.respond(HttpStatusCode.Accepted)
            }

            post("/v2/change/sync") {
                val change = call.receive<Change>()
                val timeout = Duration.parse(
                    call.request.queryParameters["timeout"]
                        ?: DEFAULT_SYNC_TIMEOUT
                )

                val result: ChangeResult
                try {
                    withTimeout(timeout) {
                        result = addChange(change).await()
                    }
                } catch (e: TimeoutCancellationException) {
                    return@post call.respond(HttpStatusCode.RequestTimeout)
                }

                return@post when (result.status) {
                    ChangeResult.Status.SUCCESS -> {
                        call.respond(HttpStatusCode.Created)
                    }

                    ChangeResult.Status.CONFLICT -> {
                        call.respond(HttpStatusCode.Conflict)
                    }

                    ChangeResult.Status.TIMEOUT -> {
                        call.respond(HttpStatusCode.RequestTimeout)
                    }
                }
            }

            get("/v2/change/{id}") {
                val id = call.parameters["id"]!!
                val entry = history.getEntryFromHistory(id)

                return@get call.respond(entry?.let { Change.fromHistoryEntry(it) }
                    ?: HttpStatusCode.NotFound)
            }

            get("/v2/change") {
                return@get call.respond(Changes.fromHistory(history))
            }
        }
    }
}
