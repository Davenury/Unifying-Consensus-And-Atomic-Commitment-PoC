package com.github.davenury.tests

import com.github.davenury.common.meterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

object Metrics {

    private val changeIdToInstant = mutableMapOf<String, Instant>()
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

    fun startTimer(changeId: String) {
        changeIdToInstant[changeId] = Instant.now()
    }

    fun stopTimer(changeId: String) {
        val timeElapsed = Duration.between(changeIdToInstant[changeId]!!, Instant.now())
        logger.info("Time processing of change with id: $changeId is $timeElapsed")
        Timer
            .builder("tests_change_processing_time")
            .register(meterRegistry)
            .record(timeElapsed)
    }

    private val logger = LoggerFactory.getLogger("TestsMetrics")

}