package com.github.davenury.ucac

import com.github.davenury.common.Change
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.commitment.gpac.Transaction
import com.github.davenury.common.PeerAddress

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
    ConsensusFollowerHeartbeatReceived,
    ConsensusFollowerChangeAccepted,
    ConsensusFollowerChangeProposed,
    ConsensusTryToBecomeLeader,
    TwoPCOnChangeApplied,
    TwoPCOnChangeAccepted,
    TwoPCBeforeProposePhase,
    TwoPCOnHandleDecision,
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
        peers: List<List<PeerAddress>>,
        transaction: Transaction? = null,
        change: Change? = null,
        historyEntry: HistoryEntry? = null
    ) {
        listeners[signal]?.onSignal(
            SignalData(
                signal,
                subject,
                peers,
                transaction,
                change,
                historyEntry
            )
        )
    }
}

data class SignalData(
    val signal: Signal,
    val subject: SignalSubject,
    val peers: List<List<PeerAddress>>,
    val transaction: Transaction?,
    val change: Change? = null,
    val historyEntry: HistoryEntry? = null
)

fun interface SignalListener {
    fun onSignal(data: SignalData)
}
