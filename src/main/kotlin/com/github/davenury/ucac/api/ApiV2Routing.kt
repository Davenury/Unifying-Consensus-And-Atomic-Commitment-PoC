package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.ucac.common.structure.HttpSubscriber
import com.github.davenury.ucac.common.structure.Subscriber
import com.github.davenury.ucac.common.structure.Subscribers
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

    suspend fun createProcessorJob(peersetId: PeersetId, call: ApplicationCall): ProcessorJob {
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
            val peersetId = call.peersetId()
            val processorJob = createProcessorJob(peersetId, call)
            service.addChange(peersetId, processorJob)

            call.respond(HttpStatusCode.Accepted)
        }

        post("/v2/change/sync") {
            val peersetId = call.peersetId()
            val processorJob = createProcessorJob(peersetId, call)
            val timeout = call.request.queryParameters["timeout"]?.let { Duration.parse(it) }

            val result: ChangeResult? = service.addChangeSync(peersetId, processorJob, timeout)
            respondChangeResult(result, call)
        }

        get("/v2/change/{id}") {
            val peersetId = call.peersetId()
            val id = call.parameters["id"]!!
            val change = service.getChangeById(peersetId, id)
            return@get call.respond(change ?: HttpStatusCode.NotFound)
        }

        get("/v2/change_status/{id}") {
            val peersetId = call.peersetId()
            val id = call.parameters["id"]!!
            val result: ChangeResult? = service.getChangeStatus(peersetId, id).getNow(null)
            respondChangeResult(result, call)
        }

        get("/v2/change") {
            val peersetId = call.peersetId()
            call.respond(service.getChanges(peersetId))
        }

        get("/v2/last-change") {
            val peersetId = call.peersetId()
            call.respond(service.getLastChange(peersetId) ?: HttpStatusCode.NotFound)
        }

        post("/v2/subscribe-to-peer-configuration-changes") {
            val address = call.receive<SubscriberAddress>()
            val peersetId = call.peersetId()
            service.registerSubscriber(peersetId, address)
            call.respond(HttpStatusCode.OK)
        }

        post("/v2/subscribe-to-peer-configuration-changes") {
            val address = call.receive<SubscriberAddress>()
            val peersetId = call.peersetId()
            service.registerSubscriber(peersetId, address)
            call.respond(HttpStatusCode.OK)
        }

        get("/peerset-information") {
            val peersetId = call.peersetId()
            call.respond(service.getPeersetInformation(peersetId).toDto())
        }
    }
}

data class PeersetInformationDto(
    val currentConsensusLeaderId: String?,
    val peersInPeerset: List<String>
)

fun PeersetInformation.toDto() = PeersetInformationDto(
    currentConsensusLeaderId = this.currentConsensusLeader?.peerId,
    peersInPeerset = this.peersInPeerset.map { it.peerId }
)
