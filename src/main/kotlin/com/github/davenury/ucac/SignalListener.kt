package com.github.davenury.ucac

import com.github.davenury.ucac.common.Change
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
    OnHandlingApplyCommitted,
    ConsensusLeaderElected,
    ConsensusAfterProposingChange,
    ConsensusAfterHandlingHeartbeat
}


interface SignalSubject {
    fun getPeerName(): String
}

class SignalPublisher(
    private val listeners: Map<Signal, SignalListener>
) {
    fun signal(signal: Signal, subject: SignalSubject, otherPeers: List<List<String>>, transaction: Transaction?, change: Change? = null) {
        listeners[signal]?.onSignal(SignalData(
            signal,
            subject,
            otherPeers,
            transaction,
            change
        ))
    }
}

data class SignalData(
    val signal: Signal,
    val subject: SignalSubject,
    val peers: List<List<String>>,
    val transaction: Transaction?,
    val change: Change? = null
)

fun interface SignalListener {
    fun onSignal(data: SignalData)
}
