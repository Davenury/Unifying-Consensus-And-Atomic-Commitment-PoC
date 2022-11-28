package com.github.davenury.ucac.commitment.TwoPC

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class TwoPC(
    private val history: History,
    private val twoPCConfig: TwoPCConfig,
    private val ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: TwoPCProtocolClient,
    private val consensusProtocol: ConsensusProtocol,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    peerResolver: PeerResolver,
) : SignalSubject, AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)


    override suspend fun performProtocol(change: Change) {

        val mainChangeId = change.toHistoryEntry().getId()

        val acceptChange =
            TwoPCChange(change.parentId, change.peers, twoPCStatus = TwoPCStatus.ACCEPTED, change = change)

        val otherPeersets = change.peers.filter { it != myAddress() }

        val decision = proposePhase(acceptChange, mainChangeId, otherPeersets)
        decisionPhase(acceptChange, decision, otherPeersets)

        val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.CONFLICT
        changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(result))
    }


    public suspend fun handleAccept(change: Change) {
        if (change !is TwoPCChange) {
            logger.info("Received not 2PC change $change")
            throw TwoPCHandleException("Received change of not TwoPCChange in handleAccept: $change")
        }

        val changeWithProperParentId = change.copyWithNewParentId(history.getCurrentEntry().getId())
        val result = consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()

        if (result.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    private suspend fun askForDecisionChange(change: Change, iteration: Int = 0) {
        val resultChange = protocolClient.askForChangeStatus(
            change.peers.first { it != myAddress() }, change
        )

        if (resultChange == null && iteration == twoPCConfig.maxChangeRetries) throw TwoPCConflictException("We are blocked, because we didn't received decision change")

        if (resultChange != null) handleDecision(resultChange.copyWithNewParentId(change.parentId))
        else changeTimer.startCounting {
            askForDecisionChange(change, iteration + 1)
        }
    }

    public suspend fun handleDecision(change: Change) {
        signal(Signal.TwoPCOnHandleDecision, change)

        val currentProcessedChange = Change.fromHistoryEntry(history.getCurrentEntry())

        val cf: CompletableFuture<ChangeResult> = when {
            currentProcessedChange !is TwoPCChange || currentProcessedChange.twoPCStatus != TwoPCStatus.ACCEPTED -> {
                throw TwoPCHandleException("Received change in handleDecision even though we didn't received 2PC-Accept earlier")
            }

            change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED && change.change == currentProcessedChange.change -> {
                changeTimer.cancelCounting()
                checkChangeAndProposeToConsensus(change)
            }

            change == currentProcessedChange.change -> {
                changeTimer.cancelCounting()
                val updatedChange = change.copyWithNewParentId(history.getCurrentEntry().getId())
                checkChangeAndProposeToConsensus(updatedChange)
            }

            else -> throw TwoPCHandleException(
                "In 2PC handleDecision received change in different type than TwoPCChange: $change \ncurrentProcessedChange:$currentProcessedChange"
            )

        }
        cf.await()
    }

    public suspend fun getChange(changeId: String): Change = consensusProtocol.getState()
        .toEntryList()
        .find { it.getParentId() == changeId }
        ?.let { Change.fromHistoryEntry(it) }
        ?: throw ChangeDoesntExist(changeId)


    companion object {
        private val logger = LoggerFactory.getLogger("2PC")
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]


    private suspend fun proposePhase(
        acceptChange: TwoPCChange,
        mainChangeId: String,
        otherPeersets: List<String>
    ): Boolean {
        val acceptResult = checkChangeAndProposeToConsensus(acceptChange).await()

        if (acceptResult.status != ChangeResult.Status.SUCCESS) changeConflict(
            mainChangeId, "failed during processing acceptChange in 2PC"
        ) else logger.info("Change accepted locally ${acceptChange.change}")


        val decision = protocolClient.sendAccept(otherPeersets, acceptChange).all { it }

        logger.info("Decision $decision from other peerset for ${acceptChange.change}")

        return decision
    }

    private suspend fun decisionPhase(acceptChange: TwoPCChange, decision: Boolean, otherPeersets: List<String>) {
        val change = acceptChange.change
        val acceptChangeId = acceptChange.toHistoryEntry().getId()

        val commitChange = if (decision) change
        else TwoPCChange(
            change.parentId, change.peers, change.acceptNum, twoPCStatus = TwoPCStatus.ABORTED, change = change
        )
//      Asynchronous commit change to consensuses
        protocolClient.sendDecision(otherPeersets, commitChange)
        val changeResult = checkChangeAndProposeToConsensus(commitChange.copyWithNewParentId(acceptChangeId)).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS) throw TwoPCConflictException("Change failed during committing locally")

        logger.info("Decision $decision committed in all peersets $commitChange")
        signal(Signal.TwoPCOnChangeApplied, change)
    }

    private fun signal(signal: Signal, change: Change) {
        signalPublisher.signal(
            signal, this, getPeersFromChange(change), null, change
        )
    }

    private fun checkChangeCompatibility(change: Change) {
        if (!history.isEntryCompatible(change.toHistoryEntry())) {
            logger.info("Change is not compatible with history $change")
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(change: Change): CompletableFuture<ChangeResult> = change
        .also { checkChangeCompatibility(it) }
        .let { consensusProtocol.proposeChangeAsync(change) }


    private suspend fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            .also { throw TwoPCConflictException(exceptionText) }


    private fun myAddress() = peerResolver.currentPeerAddress().address

    private fun applySignal(signal: Signal, change: Change) {
        try {
            signal(signal, change)
        } catch (e: java.lang.Exception) {
            // TODO change approach to simulating errors in signal listeners
            changeTimeout(change, e.toString())
            throw e
        }
    }

    private fun changeTimeout(change: Change, detailedMessage: String? = null) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.TIMEOUT, detailedMessage))
    }
}