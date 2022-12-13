package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.TwoPCConfig
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class TwoPC(
    private val history: History,
    twoPCConfig: TwoPCConfig,
    ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: TwoPCProtocolClient,
    private val consensusProtocol: ConsensusProtocol,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    peerResolver: PeerResolver,
    private val isMetricTest: Boolean
) : SignalSubject, AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)

    override suspend fun performProtocol(change: Change) {
//        val updatedChange =
//            updateParentIdFor2PCCompatibility(change, history, peerResolver.currentPeer().peersetId)

        val mainChangeId = change.id
        try {
            val acceptChange = TwoPCTransition(
                twoPCStatus = TwoPCStatus.ACCEPTED,
                change = change,
            )

            val otherPeers = change.peersets
                .map { it.peersetId }
                .filter { it != peerResolver.currentPeer().peersetId }
                .map { peerResolver.resolve(GlobalPeerId(it, 0)) }

            signal(Signal.TwoPCBeforeProposePhase, change)
            val decision = proposePhase(acceptChange, mainChangeId, otherPeers)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = mainChangeId,
                    peerId = peerResolver.currentPeer().peerId,
                    peersetId = peerResolver.currentPeer().peersetId,
                    protocolName = ProtocolName.TWO_PC,
                    state = "proposed_decision_$decision"
                )
            }

            signal(Signal.TwoPCOnChangeAccepted, change)
            decisionPhase(acceptChange, decision, otherPeers)

            val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.ABORTED

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = mainChangeId,
                    peerId = peerResolver.currentPeer().peerId,
                    peersetId = peerResolver.currentPeer().peersetId,
                    protocolName = ProtocolName.TWO_PC,
                    state = result.name.lowercase()
                )
            }

            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(result))
        } catch (e: Exception) {
            changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
        }
    }

    suspend fun handleAccept(transition: Transition) {
        if (transition !is TwoPCTransition) {
            logger.info("Received not 2PC transition $transition")
            throw TwoPCHandleException("Received transition which is not ${TwoPCTransition::class}: $transition")
        }

//        val changeWithProperParentId = change.copyWithNewParentId(
//            peerResolver.currentPeer().peersetId,
//            history.getCurrentEntry().getId(),
//        )
        val result = consensusProtocol.proposeTransitionAsync(transition).await()

        if (result.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        changeTimer.startCounting {
            askForDecisionChange(transition)
        }
    }

    private suspend fun askForDecisionChange(transition: Transition) {
        val resultTransition = protocolClient.askForChangeStatus(
            transition.change.peersets.map { it.peersetId }
                .first { it != peerResolver.currentPeer().peersetId }
                .let { peerResolver.resolve(GlobalPeerId(it, 0)) },
            transition
        )

        if (resultTransition != null) {
            handleDecision(resultTransition)
        } else changeTimer.startCounting {
            askForDecisionChange(transition)
        }
    }

    suspend fun handleDecision(transition: Transition) {
        signal(Signal.TwoPCOnHandleDecision, transition.change)

        val currentProcessedChange = Transition.fromHistoryEntry(history.getCurrentEntry())

        val cf: CompletableFuture<ChangeResult> = when {
            currentProcessedChange !is TwoPCTransition || currentProcessedChange.twoPCStatus != TwoPCStatus.ACCEPTED -> {
                throw TwoPCHandleException("Received change in handleDecision even though we didn't received 2PC-Accept earlier")
            }

            transition is TwoPCTransition && transition.twoPCStatus == TwoPCStatus.ABORTED && transition.change == currentProcessedChange.change -> {
                changeTimer.cancelCounting()
                checkChangeAndProposeToConsensus(transition, currentProcessedChange.change.id)
            }

            transition.change == currentProcessedChange.change -> {
                changeTimer.cancelCounting()
//                val updatedChange = change.copyWithNewParentId(
//                    peerResolver.currentPeer().peersetId,
//                    history.getCurrentEntry().getId(),
//                )
                checkChangeAndProposeToConsensus(transition, currentProcessedChange.change.id)
            }

            else -> throw TwoPCHandleException(
                "In 2PC handleDecision received change in different type than " +
                        "${TwoPCTransition::class}: $transition , " +
                        "currentProcessedChange: $currentProcessedChange"
            )
        }
        cf.await()
    }

    fun getChange(changeId: String): Transition {
        val entryList = consensusProtocol.getState().toEntryList()

        val parentEntry = entryList
            .find { Transition.fromHistoryEntry(it)?.change?.id == changeId }
            ?: throw ChangeDoesntExist(changeId)

        val transition = entryList
            .find { it.getParentId() == parentEntry.getId() }
            ?.let { Transition.fromHistoryEntry(it) }
            ?: throw ChangeDoesntExist(changeId)

        // TODO hax, remove it
//        return if (transition is TwoPCTransition) {
//            transition
//        } else {
//            Transition.fromHistoryEntry(parentEntry)
//                .let {
//                    if (it !is TwoPCTransition) {
//                        throw ChangeDoesntExist(changeId)
//                    } else {
//                        it
//                    }
//                }.change
//        }
        return transition
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    private suspend fun proposePhase(
        acceptChange: TwoPCTransition,
        mainChangeId: String,
        otherPeers: List<PeerAddress>
    ): Boolean {
        val acceptResult = checkChangeAndProposeToConsensus(acceptChange, mainChangeId).await()

        if (acceptResult.status != ChangeResult.Status.SUCCESS) {
            changeConflict(mainChangeId, "failed during processing acceptChange in 2PC")
        } else {
            logger.info("Change accepted locally ${acceptChange.change}")
        }

        val decision = protocolClient.sendAccept(otherPeers, acceptChange).all { it }

        logger.info("Decision $decision from other peerset for ${acceptChange.change}")

        return decision
    }

    private suspend fun decisionPhase(
        acceptTransition: TwoPCTransition,
        decision: Boolean,
        otherPeersets: List<PeerAddress>,
    ) {

        val change = acceptTransition.change
        val acceptChangeId = acceptTransition.toHistoryEntry(peerResolver.currentPeer().peersetId).getId()

        val commitChange = if (decision) {
            acceptTransition
        } else {
            TwoPCTransition(
                twoPCStatus = TwoPCStatus.ABORTED,
                change = change,
            )
        }
//      Asynchronous commit change to consensuses

        protocolClient.sendDecision(otherPeersets, commitChange)
        val changeResult = checkChangeAndProposeToConsensus(
            acceptTransition,
            acceptTransition.change.id
        ).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCConflictException("Change failed during committing locally")
        }

        logger.info("Decision $decision committed in all peersets $commitChange")
        signal(Signal.TwoPCOnChangeApplied, change)
    }

    private fun signal(signal: Signal, change: Change) {
        signalPublisher.signal(
            signal = signal,
            subject = this,
            peers = getPeersFromChange(change),
            transaction = null,
            change = change,
        )
    }

    private fun checkTransitionCompatibility(transition: Transition, originalChangeId: String) {
        val peerset = peerResolver.currentPeer().peersetId
        if (!history.isEntryCompatible(transition.toHistoryEntry(peerset))) {
            logger.info(
                "Change $originalChangeId is not compatible with history expected: ${
                    transition.toHistoryEntry(
                        peerset
                    ).getParentId()
                } is ${history.getCurrentEntry().getId()}"
            )
            changeIdToCompletableFuture[originalChangeId]!!.complete(ChangeResult(ChangeResult.Status.REJECTED))
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(
        transition: Transition,
        originalChangeId: String
    ): CompletableFuture<ChangeResult> = transition
        .also { checkTransitionCompatibility(it, originalChangeId) }
        .let { consensusProtocol.proposeTransitionAsync(transition) }


    private fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            .also { throw TwoPCConflictException(exceptionText) }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc")


        fun updateParentIdFor2PCCompatibility(change: Change, history: History, peersetId: Int): Change {
            val currentEntry = history.getCurrentEntry()

            val grandParentTransition: Transition =
                history.getEntryFromHistory(currentEntry.getParentId() ?: return change)
                    ?.let { Transition.fromHistoryEntry(it) }
                    ?: return change

            if (grandParentTransition !is TwoPCTransition){
                return change
            }

            val proposedChangeParentId = ChangeApplyingTransition(change)
                .toHistoryEntry(peersetId)
                .getParentId()

            val grandParentChange2PCChangeId = ChangeApplyingTransition(grandParentTransition.change)
                .toHistoryEntry(peersetId)
                .getId()

            return if (grandParentChange2PCChangeId == proposedChangeParentId) change.copyWithNewParentId(
                peersetId,
                history.getCurrentEntry().getId()
            )
            else change
        }
    }
}
