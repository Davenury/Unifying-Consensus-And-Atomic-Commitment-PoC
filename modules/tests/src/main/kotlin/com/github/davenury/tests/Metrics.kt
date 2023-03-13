package com.github.davenury.tests

import com.github.davenury.common.meterRegistry
import io.micrometer.core.instrument.Tag

object Metrics {

    fun reportUnsuccessfulChange(code: Int) {
        meterRegistry.counter("unsuccessful_change", listOf(Tag.of("code", "$code"))).increment()
    }

    fun bumpSentChanges() {
        meterRegistry.counter("sent_changes").increment()
    }

    fun bumpDelayInSendingChange() {
        meterRegistry.counter("sending_change_delay").increment()
    }
}
