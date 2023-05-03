package com.github.davenury.ucac

import com.github.davenury.common.Change
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.commitment.gpac.Transaction
import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver

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
    ConsensusLeaderIHaveBeenElected,
    ConsensusLeaderElected,
    ConsensusLeaderDoesNotSendHeartbeat,
    ConsensusAfterProposingChange,
    ConsensusFollowerHeartbeatReceived,
    ConsensusFollowerChangeAccepted,
    ConsensusFollowerChangeProposed,
    ConsensusTryToBecomeLeader,
    ConsensusSendHeartbeat,
    TwoPCOnChangeApplied,
    TwoPCOnChangeAccepted,
    TwoPCBeforeProposePhase,
    TwoPCOnHandleDecision,
    TwoPCOnAskForDecision,
    TwoPCOnHandleDecisionEnd,
    AlvinCommitChange,
    AlvinAbortChange,
    AlvinReceiveProposal,
    AlvinAfterProposalPhase,
    AlvinAfterAcceptPhase,
    AlvinAfterStablePhase,
    PigPaxosLeaderElected,
    PigPaxosChangeCommitted,
    PigPaxosChangeAborted,
    PigPaxosTryToBecomeLeader,
    PigPaxosAfterAcceptChange,
    PigPaxosReceivedAccept,
    PigPaxosReceivedCommit,
}


interface SignalSubject {
    fun getPeerName(): String
}

class SignalPublisher(
    private val listeners: Map<Signal, SignalListener>,
    private val peerResolver: PeerResolver,
) {
    fun signal(
        signal: Signal,
        subject: SignalSubject,
        peers: Map<PeersetId, List<PeerAddress>>,
        transaction: Transaction? = null,
        change: Change? = null,
        historyEntry: HistoryEntry? = null
    ) {
        listeners[signal]?.onSignal(
            SignalData(
                signal,
                subject,
                peerResolver,
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
    val peerResolver: PeerResolver,
    val peers: Map<PeersetId, List<PeerAddress>>,
    val transaction: Transaction?,
    val change: Change? = null,
    val historyEntry: HistoryEntry? = null
)

fun interface SignalListener {
    fun onSignal(data: SignalData)
}
