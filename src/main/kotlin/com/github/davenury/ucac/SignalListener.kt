package com.github.davenury.ucac

import com.github.davenury.ucac.gpac.domain.Transaction

enum class Signal {
    BeforeSendingElect,
    OnHandlingElectBegin,
    OnHandlingElectEnd,
    BeforeSendingAgree,
    OnHandlingAgreeBegin,
    OnHandlingAgreeEnd,
    BeforeSendingApply,
    OnHandlingApplyBegin,
    OnHandlingApplyEnd,
    ConsensusAfterProposingChange
}


interface SignalSubject

class SignalPublisher(
    private val listeners: Map<Signal, SignalListener>
) {
    fun signal(signal: Signal, subject: SignalSubject, otherPeers: List<List<String>>, transaction: Transaction?) {
        listeners[signal]?.onSignal(SignalData(
            signal,
            subject,
            otherPeers,
            transaction,
        ))
    }
}

data class SignalData(
    val signal: Signal,
    val subject: SignalSubject,
    val otherPeers: List<List<String>>,
    val transaction: Transaction?,
)

fun interface SignalListener {
    fun onSignal(data: SignalData)
}
