package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.zopa.ktor.opentracing.span
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class TwoPC(
    private val peersetId: PeersetId,
    private val history: History,
    twoPCConfig: TwoPCConfig,
    ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: TwoPCProtocolClient,
    private val consensusProtocol: ConsensusProtocol,
    peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val isMetricTest: Boolean,
    private val changeNotifier: ChangeNotifier,
) : SignalSubject, AbstractAtomicCommitmentProtocol(logger, peerResolver) {
    private val peerId = peerResolver.currentPeer()

    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)

    override suspend fun performProtocol(change: Change): Unit = span("TwoPC.performProtocol") {
        val updatedChange =
            updateParentIdFor2PCCompatibility(change, history, peersetId)

        val mainChangeId = updatedChange.id
        try {
            val acceptChange = TwoPCChange(
                peersets = updatedChange.peersets,
                twoPCStatus = TwoPCStatus.ACCEPTED,
                change = change,
            )

            val otherPeers = updatedChange.peersets
                .map { it.peersetId }
                .filter { it != peersetId }
                .map { peerResolver.getPeersFromPeerset(it)[0] }

            signal(Signal.TwoPCBeforeProposePhase, change)
            val decision = proposePhase(acceptChange, mainChangeId, otherPeers)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = mainChangeId,
                    peerId = peerId,
                    peersetId = peersetId,
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
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.TWO_PC,
                    state = result.name.lowercase()
                )
            }

            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(result))
            signal(Signal.TwoPCOnChangeApplied, change)
        } catch (e: Exception) {
            changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
        }
    }

    suspend fun handleAccept(change: Change) = span("TwoPc.handleAccept") {
        if (change !is TwoPCChange) {
            logger.info("Received not 2PC change $change")
            throw TwoPCHandleException("Received change of not TwoPCChange in handleAccept: $change")
        }

        logger.info("Change id for change: $change, id: ${change.change.id}")
        changeIdToCompletableFuture.putIfAbsent(change.change.id, CompletableFuture<ChangeResult>())

        val changeWithProperParentId = change.copyWithNewParentId(
            peersetId,
            history.getCurrentEntryId(),
        )
        val result = consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()

        if (result.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    private suspend fun askForDecisionChange(change: Change): Unit = span("TwoPc.askForDecisionChange") {
        val resultChange = protocolClient.askForChangeStatus(
            change.peersets.map { it.peersetId }
                .first { it != peersetId }
                .let { peerResolver.getPeersFromPeerset(it)[0] },
            change
        )

        if (resultChange != null) {
            handleDecision(resultChange)
        } else changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    suspend fun handleDecision(change: Change): Unit = span("TwoPc.handleDecision") {
        signal(Signal.TwoPCOnHandleDecision, change)
        val mainChangeId =
            updateParentIdFor2PCCompatibility(change, history, peersetId).id
        logger.info("Change id for change: $change, id: $mainChangeId")

        val currentProcessedChange = Change.fromHistoryEntry(history.getCurrentEntry())

        try {
            val cf: CompletableFuture<ChangeResult> = when {
                currentProcessedChange !is TwoPCChange || currentProcessedChange.twoPCStatus != TwoPCStatus.ACCEPTED -> {
                    throw TwoPCHandleException("Received change in handleDecision even though we didn't received 2PC-Accept earlier")
                }

                change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED && change.change == currentProcessedChange.change -> {
                    changeTimer.cancelCounting()
                    checkChangeAndProposeToConsensus(change, currentProcessedChange.change.id)
                }

                change == currentProcessedChange.change -> {
                    changeTimer.cancelCounting()
                    val updatedChange = change.copyWithNewParentId(
                        peersetId,
                        history.getCurrentEntryId(),
                    )
                    checkChangeAndProposeToConsensus(updatedChange, currentProcessedChange.change.id)
                }

                else -> throw TwoPCHandleException(
                    "In 2PC handleDecision received change in different type than TwoPCChange: $change \n" +
                            "currentProcessedChange: $currentProcessedChange"
                )
            }
            cf.await()
        } catch (e: Exception) {
            changeConflict(mainChangeId, "Change conflicted in decision phase, ${e.message}")
            throw e
        }

        changeIdToCompletableFuture[change.id]!!
            .complete(ChangeResult(ChangeResult.Status.SUCCESS))
        changeNotifier.notify(change, ChangeResult(ChangeResult.Status.SUCCESS))
    }

    fun getChange(changeId: String): Change {
        val entryList = consensusProtocol.getState().toEntryList()

        val parentEntry = entryList
            .find { Change.fromHistoryEntry(it)?.id == changeId }
            ?: throw ChangeDoesntExist(changeId)

        val change = entryList
            .find { it.getParentId() == parentEntry.getId() }
            ?.let { Change.fromHistoryEntry(it) }
            ?: throw ChangeDoesntExist(changeId)

        // TODO hax, remove it
        return if (change is TwoPCChange) {
            change
        } else {
            Change.fromHistoryEntry(parentEntry)
                .let {
                    if (it !is TwoPCChange) {
                        throw ChangeDoesntExist(changeId)
                    } else {
                        it
                    }
                }.change
        }
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    private suspend fun proposePhase(
        acceptChange: TwoPCChange,
        mainChangeId: String,
        otherPeers: List<PeerAddress>
    ): Boolean = span("TwoPc.proposePhase") {
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
        acceptChange: TwoPCChange,
        decision: Boolean,
        otherPeersets: List<PeerAddress>,
    ): Unit = span("TwoPc.decisionPhase") {

        val change = acceptChange.change
        val acceptChangeId = acceptChange.toHistoryEntry(peersetId).getId()

        val commitChange = if (decision) {
            change
        } else {
            TwoPCChange(
                peersets = change.peersets,
                twoPCStatus = TwoPCStatus.ABORTED,
                change = change,
            )
        }
//      Asynchronous commit change to consensuses

        protocolClient.sendDecision(otherPeersets, commitChange)
        val changeResult = checkChangeAndProposeToConsensus(
            commitChange.copyWithNewParentId(
                peersetId,
                acceptChangeId,
            ),
            acceptChange.change.id
        ).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCConflictException("Change failed during committing locally")
        }

        logger.info("Decision $decision committed in all peersets $commitChange")
    }

    private fun signal(signal: Signal, change: Change) {
        signalPublisher.signal(
            signal, this, getPeersFromChange(change), null, change
        )
    }

    private fun checkChangeCompatibility(change: Change, originalChangeId: String) {
        if (!history.isEntryCompatible(change.toHistoryEntry(peersetId))) {
            logger.info(
                "Change $originalChangeId is not compatible with history expected: ${
                    change.toHistoryEntry(peersetId).getParentId()
                } is ${history.getCurrentEntryId()}"
            )
            changeIdToCompletableFuture[originalChangeId]!!.complete(ChangeResult(ChangeResult.Status.REJECTED))
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(
        change: Change,
        originalChangeId: String
    ): CompletableFuture<ChangeResult> = change
        .also { checkChangeCompatibility(it, originalChangeId) }
        .let { consensusProtocol.proposeChangeAsync(change) }


    private fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            .also { throw TwoPCConflictException(exceptionText) }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc")

        fun updateParentIdFor2PCCompatibility(change: Change, history: History, peersetId: PeersetId): Change {
            val currentEntry = history.getCurrentEntry()

            val grandParentChange: Change =
                history.getEntryFromHistory(currentEntry.getParentId() ?: return change)
                    ?.let { Change.fromHistoryEntry(it) }
                    ?: return change

            if (grandParentChange !is TwoPCChange) return change

            val proposedChangeParentId = change.toHistoryEntry(peersetId)
                .getParentId()

            val grandParentChange2PCChangeId = grandParentChange.change.toHistoryEntry(peersetId).getId()

            return if (grandParentChange2PCChangeId == proposedChangeParentId) change.copyWithNewParentId(
                peersetId,
                history.getCurrentEntryId()
            )
            else change
        }

    }
}