package com.github.davenury.tests

import com.github.davenury.common.meterRegistry

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

}