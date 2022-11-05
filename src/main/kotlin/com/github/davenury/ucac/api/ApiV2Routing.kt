package com.github.davenury.ucac.api

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeCreationErrorMessage
import com.github.davenury.ucac.common.ChangeResult
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import java.time.Duration

fun Application.apiV2Routing(
    service: ApiV2Service,
) {
    suspend fun respondChangeResult(result: ChangeResult?, call: ApplicationCall) {
        when (result?.status) {
            ChangeResult.Status.SUCCESS -> {
                call.respond(
                    HttpStatusCode.Accepted,
                    ChangeCreationErrorMessage(
                        "Change applied",
                        changeApplied = "true",
                    ),
                )
            }

            ChangeResult.Status.CONFLICT -> {
                call.respond(
                    HttpStatusCode.Conflict,
                    ChangeCreationErrorMessage(
                        "Change conflicted",
                        changeApplied = "false",
                    ),
                )
            }

            ChangeResult.Status.TIMEOUT -> {
                call.respond(
                    HttpStatusCode.RequestTimeout,
                    ChangeCreationErrorMessage(
                        "Change not applied due to timeout",
                        changeApplied = "false",
                    ),
                )
            }

            ChangeResult.Status.EXCEPTION -> throw result.exception!!

            else -> {
                call.respond(
                    HttpStatusCode.RequestTimeout,
                    ChangeCreationErrorMessage(
                        "Change conflicted",
                        changeApplied = "unknown",
                    ),
                )
            }
        }
    }

    routing {
        post("/v2/change/async") {
            val enforceGpac: Boolean = call.request.queryParameters["enforce_gpac"] != null
            val change = call.receive<Change>()
            service.addChange(change, enforceGpac)
            call.respond(HttpStatusCode.Created)
        }

        post("/v2/change/sync") {
            val enforceGpac: Boolean = call.request.queryParameters["enforce_gpac"] != null
            val change = call.receive<Change>()
            val timeout = call.request.queryParameters["timeout"]
                ?.let { Duration.parse(it) }

            val result: ChangeResult? = service.addChangeSync(change, enforceGpac, timeout)
            respondChangeResult(result, call)
        }

        get("/v2/change/{id}") {
            val id = call.parameters["id"]!!
            val change = service.getChangeById(id)
            return@get call.respond(change ?: HttpStatusCode.NotFound)
        }

        get("/v2/change_status/{id}") {
            val id = call.parameters["id"]!!

            val result = service.getChangeStatus(id).get()
            respondChangeResult(result, call)
        }

        get("/v2/change") {
            call.respond(service.getChanges())
        }
    }
}
