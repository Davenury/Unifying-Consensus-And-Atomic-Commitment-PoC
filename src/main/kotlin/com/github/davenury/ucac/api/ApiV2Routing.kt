package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeCreationResponse
import com.github.davenury.common.ChangeCreationStatus
import com.github.davenury.common.ChangeResult
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import java.time.Duration
import java.util.concurrent.CompletableFuture

fun Application.apiV2Routing(
    service: ApiV2Service,
) {
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

            else -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Timed out while waiting for change",
                        detailedMessage = null,
                        changeStatus = ChangeCreationStatus.UNKNOWN,
                    ),
                )
            }
        }
    }

    suspend fun getProcessorJob(call: ApplicationCall): ProcessorJob {
        val enforceGpac: Boolean = call.request.queryParameters["enforce_gpac"]?.toBoolean() ?: false
        val useTwoPC: Boolean = call.request.queryParameters["use_2pc"]?.toBoolean() ?: false
        val change = call.receive<Change>()

        val isOnePeersetChange = service.allPeersFromMyPeerset(change.peers)

        val processorJobType = when {
            isOnePeersetChange && !enforceGpac -> ProcessorJobType.CONSENSUS
            useTwoPC -> ProcessorJobType.TWO_PC
            else -> ProcessorJobType.GPAC
        }

//        println("enforceGpac: $enforceGpac \nuse2pc: $useTwoPC\nisOnePeersetChange: $isOnePeersetChange\njobType: $processorJobType")

        return ProcessorJob(change, CompletableFuture(), processorJobType)
    }

    routing {
        post("/v2/change/async") {
            val processorJob = getProcessorJob(call)
            service.addChange(processorJob)
            call.respond(HttpStatusCode.Accepted)
        }

        post("/v2/change/sync") {
            val processorJob = getProcessorJob(call)
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
    }
}
