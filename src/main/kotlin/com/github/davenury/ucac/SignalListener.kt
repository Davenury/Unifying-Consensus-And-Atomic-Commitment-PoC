package com.github.davenury.ucac

import com.github.davenury.common.Change
import com.github.davenury.ucac.commitment.gpac.Transaction

enum class Signal {
    BeforeSendingElect,
    OnSendingElectBuildFail,
    OnHandlingElectBegin,
    OnHandlingElectEnd,
    BeforeSendingAgree,
    OnHandlingAgreeBegin,
    OnHandlingAgreeEnd,
    BeforeSendingApply,
    OnHandlingApplyBegin,
    OnHandlingApplyEnd,
    OnHandlingApplyCommitted,
    ReachedMaxRetries,
    ConsensusLeaderElected,
    ConsensusLeaderDoesNotSendHeartbeat,
    ConsensusAfterProposingChange,
    ConsensusFollowerChangeAccepted,
    ConsensusFollowerChangeProposed,
    ConsensusTryToBecomeLeader
}


interface SignalSubject {
    fun getPeerName(): String
}

class SignalPublisher(
    private val listeners: Map<Signal, SignalListener>
) {
    fun signal(
        signal: Signal,
        subject: SignalSubject,
        otherPeers: List<List<String>>,
        transaction: Transaction?,
        change: Change? = null,
    ) {
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
