package com.example

import com.example.gpac.domain.Transaction

typealias AdditionalAction = suspend (Transaction?) -> Unit

enum class TestAddon {
    BeforeSendingElect,
    OnHandlingElectBegin,
    OnHandlingElectEnd,
    BeforeSendingAgree,
    OnHandlingAgreeBegin,
    OnHandlingAgreeEnd,
    BeforeSendingApply,
    OnHandlingApplyBegin,
    OnHandlingApplyEnd
}

data class Signal(val addon: TestAddon)

interface SignalSubject

class EventPublisher(
    private val listeners: List<EventListener>
) {
    fun signal(addon: TestAddon, subject: SignalSubject) {
        listeners.forEach { it.onSignal(Signal(addon), subject) }
    }
}

interface EventListener {
    fun onSignal(signal: Signal, subject: SignalSubject)
}