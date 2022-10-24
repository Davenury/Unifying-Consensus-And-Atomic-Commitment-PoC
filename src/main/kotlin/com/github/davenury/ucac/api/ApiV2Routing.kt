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
    routing {
        post("/v2/change/async") {
            val change = call.receive<Change>()
            service.addChange(change)
            call.respond(HttpStatusCode.Accepted)
        }

        post("/v2/change/sync") {
            val change = call.receive<Change>()
            val timeout = call.request.queryParameters["timeout"]
                ?.let { Duration.parse(it) }

            val result: ChangeResult? = service.addChangeSync(change, timeout)
            return@post when (result?.status) {
                ChangeResult.Status.SUCCESS -> {
                    call.respond(
                        HttpStatusCode.Created,
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

        get("/v2/change/{id}") {
            val id = call.parameters["id"]!!
            val change = service.getChangeById(id)
            return@get call.respond(change ?: HttpStatusCode.NotFound)
        }

        get("/v2/change") {
            call.respond(service.getChanges())
        }
    }

}
