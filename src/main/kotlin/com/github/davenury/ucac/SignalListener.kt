package com.github.davenury.ucac

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeApplyingTransition
import com.github.davenury.common.Transition
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.commitment.gpac.Transaction
import com.github.davenury.ucac.common.PeerAddress

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
    ConsensusFollowerTransitionAccepted,
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
        transition: Transition? = null,
        historyEntry: HistoryEntry? = null
    ) {
        listeners[signal]?.onSignal(
            SignalData(
                signal,
                subject,
                peers,
                transaction,
                change,
                transition,
                historyEntry,
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
    val transition: Transition? = null,
    val historyEntry: HistoryEntry? = null
)

fun interface SignalListener {
    fun onSignal(data: SignalData)
}
