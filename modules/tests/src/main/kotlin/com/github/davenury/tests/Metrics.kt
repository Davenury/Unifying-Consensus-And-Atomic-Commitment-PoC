package com.github.davenury.tests

import com.github.davenury.common.meterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

object Metrics {

    fun reportUnsuccessfulChange(code: Int) {
        meterRegistry.counter("unsuccessful_change", "$code").increment()
    }

    fun bumpSentChanges() {
        meterRegistry.counter("sent_changes").increment()
    }

    fun bumpDelayInSendingChange() {
        meterRegistry.counter("sending_change_delay").increment()
    }

    fun bumpConflictedChanges(message: String?) {
        meterRegistry.counter("conflicted_changes", listOf(Tag.of("detailed_message", message ?: "null"))).increment()
    }

}