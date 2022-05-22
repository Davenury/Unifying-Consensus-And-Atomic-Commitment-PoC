package com.example

import com.example.domain.Transaction
import io.ktor.application.*

data class GPACEventData(
    val action: String,
    val transaction: Transaction
)

val GPACEventDefinition = EventDefinition<GPACEventData>()

fun Application.raiseEvent(eventData: GPACEventData) {
    environment.monitor.raise(GPACEventDefinition, eventData)
}

fun Application.subscribeToEvent(handler: (GPACEventData) -> Unit) {
    environment.monitor.subscribe(GPACEventDefinition, handler)
}