package com.github.davenury.ucac

import com.github.davenury.ucac.gpac.domain.Transaction

typealias AdditionalAction = suspend (protocolTestInformation: ProtocolTestInformation) -> Unit

data class ProtocolTestInformation(
    val transaction: Transaction?,
    val otherPeers: List<List<String>>
)

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
    fun signal(addon: TestAddon, subject: SignalSubject, otherPeers: List<List<String>>) {
        listeners.forEach { it.onSignal(Signal(addon), subject, otherPeers) }
    }
}

interface EventListener {
    fun onSignal(signal: Signal, subject: SignalSubject, otherPeers: List<List<String>>)
}