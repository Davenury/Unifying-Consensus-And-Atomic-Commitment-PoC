package com.github.davenury.ucac.api

import com.github.davenury.common.*
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

fun Application.apiV2Routing(
    service: ApiV2Service,
    peersetId: PeersetId,
) {
    val logger = LoggerFactory.getLogger("V2Routing")

    suspend fun respondChangeResult(result: ChangeResult?, call: ApplicationCall) {
        when (result?.status) {
            ChangeResult.Status.SUCCESS -> {
                call.respond(
                    HttpStatusCode.Created,
                    ChangeCreationResponse(
                        "Change applied",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.APPLIED,
                    ),
                )
            }

            ChangeResult.Status.CONFLICT -> {
                call.respond(
                    HttpStatusCode.Conflict,
                    ChangeCreationResponse(
                        "Change conflicted",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.NOT_APPLIED,
                    ),
                )
            }

            ChangeResult.Status.TIMEOUT -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Change not applied due to timeout",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.NOT_APPLIED,
                    ),
                )
            }

            null -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Timed out while waiting for change (changeResult is null)",
                        detailedMessage = null,
                        changeStatus = ChangeCreationStatus.UNKNOWN,
                    ),
                )
            }
        }
    }

    suspend fun createProcessorJob(call: ApplicationCall): ProcessorJob {
        val enforceGpac: Boolean = call.request.queryParameters["enforce_gpac"]?.toBoolean() ?: false
        val useTwoPC: Boolean = call.request.queryParameters["use_2pc"]?.toBoolean() ?: false
        val change = call.receive<Change>()

        val isOnePeersetChange = when (change.peersets.size) {
            0 -> throw IllegalArgumentException("Change needs at least one peerset")
            1 -> true
            else -> false
        }

        if (change.peersets.find { it.peersetId == peersetId } == null) {
            throw IllegalArgumentException("My peerset ID is not in the change")
        }

        val protocolName = when {
            isOnePeersetChange && !enforceGpac -> ProtocolName.CONSENSUS
            useTwoPC -> ProtocolName.TWO_PC
            else -> ProtocolName.GPAC
        }

        logger.info("Create ProcessorJob from parameters: (isOnePeersetChange=$isOnePeersetChange, enforceGPAC: $enforceGpac, 2PC: $useTwoPC), resultType: $protocolName")

        return ProcessorJob(change, CompletableFuture(), protocolName)
    }

    routing {
        post("/v2/change/async") {
            val processorJob = createProcessorJob(call)
            service.addChange(processorJob)

            call.respond(HttpStatusCode.Accepted)
        }

        post("/v2/change/sync") {
            val processorJob = createProcessorJob(call)
            val timeout = call.request.queryParameters["timeout"]?.let { Duration.parse(it) }

            val result: ChangeResult? = service.addChangeSync(processorJob, timeout)
            respondChangeResult(result, call)
        }

        get("/v2/change/{id}") {
            val id = call.parameters["id"]!!
            val change = service.getChangeById(id)
            return@get call.respond(change ?: HttpStatusCode.NotFound)
        }

        get("/v2/change_status/{id}") {
            val id = call.parameters["id"]!!
            val result: ChangeResult? = service.getChangeStatus(id).getNow(null)
            respondChangeResult(result, call)
        }

        get("/v2/change") {
            call.respond(service.getChanges())
        }

        get("/v2/last-change") {
            call.respond(service.getLastChange() ?: HttpStatusCode.NotFound)
        }
    }
}
