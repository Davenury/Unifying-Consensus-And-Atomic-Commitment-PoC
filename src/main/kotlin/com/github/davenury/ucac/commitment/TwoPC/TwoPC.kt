package com.github.davenury.ucac.commitment.TwoPC

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
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
    private val myPeersetId: Int,
    private val myNodeId: Int,
    private val peerResolver: PeerResolver
) : SignalSubject, AtomicCommitmentProtocol {

    private var currentProcessedChange: TwoPCChange? = null
    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)

    private val mutex = Mutex()

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    override fun getPeerName() = "peerset${myPeersetId}/peer${myNodeId}"

    override suspend fun performProtocol(change: Change) {


        val mainChangeId = change.toHistoryEntry().getId()

        val acceptChange = TwoPCChange(change.parentId, TwoPCStatus.ACCEPTED, change.peers, change = change)

        mutex.withLock {
            currentProcessedChange = acceptChange
        }

        val acceptResult = checkChangeAndProposeToConsensus(acceptChange).await()

        if (acceptResult.status != ChangeResult.Status.SUCCESS) changeConflict(
            mainChangeId,
            "failed during processing acceptChange"
        ) else
            logger.info("Change accepted locally $change")

        val otherPeersets = change.peers.filter { it != myAddress() }
        val decision = protocolClient
            .sendAccept(otherPeersets, acceptChange)
            .all { it }

        logger.info("Decision $decision from other peerset for $change")

        val acceptChangeId = acceptChange.toHistoryEntry().getId()

        val commitChange =
            if (decision) change.copyWithNewParentId(acceptChangeId)
            else TwoPCChange(acceptChangeId, TwoPCStatus.ABORTED, change.peers, change.acceptNum, change)


        logger.info("AcceptChangeId: $acceptChangeId")
        logger.info("ParentId: ${commitChange.parentId}")
//      Asynchronous commit change to consensuses
        protocolClient.sendDecision(otherPeersets, commitChange)
        val changeResult = checkChangeAndProposeToConsensus(commitChange).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS)
            throw TwoPCConflictException("Change failed during committing locally")

        logger.info("Decision $decision committed in all peersets $commitChange")
        signal(Signal.TwoPCOnChangeApplied, change)

        mutex.withLock {
            currentProcessedChange = null
        }

        val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.CONFLICT
        changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(result))
    }


    public suspend fun handleAccept(change: Change) {
        if ((change is TwoPCChange).not()) {
            logger.info("Received not 2PC change $change")
            throw TwoPCHandleException("Received change of not TwoPCChange in handleAccept: $change")
        }

        mutex.withLock {
            if (currentProcessedChange != null) throw TwoPCHandleException("Currently processing other 2PC change")
        }


        val changeWithProperParentId = change.copyWithNewParentId(history.getCurrentEntry().getId())
        val result = consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()

        if (result.status == ChangeResult.Status.SUCCESS) mutex.withLock {
            currentProcessedChange = change as TwoPCChange
        } else {
            throw TwoPCHandleException("TwoPCChange doesn't applied change")
        }

        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    public suspend fun handleDecision(change: Change) {
        signal(Signal.TwoPCOnHandleDecision, change)

        val cf: CompletableFuture<ChangeResult>
        mutex.withLock {
            val changeId = currentProcessedChange?.toHistoryEntry()?.getId()

            cf = when {
                currentProcessedChange == null -> {
                    throw TwoPCHandleException("Received change in handleDecision even though we didn't received 2PC-Accept earlier")
                }
                change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED && change.change == currentProcessedChange -> {
                    changeTimer.cancelCounting()
                    checkChangeAndProposeToConsensus(change)
                }

                change.parentId == changeId &&
                        change.copyWithNewParentId(currentProcessedChange!!.parentId) == currentProcessedChange!!.change -> {
                    changeTimer.cancelCounting()
                    val updatedChange = change.copyWithNewParentId(history.getCurrentEntry().getId())
                    checkChangeAndProposeToConsensus(updatedChange)
                }

                else -> throw TwoPCHandleException(
                    "In 2PC handleDecision received change in different type than TwoPCChange: $change \n" +
                            "currentProcessedChange:$currentProcessedChange"
                )

            }
        }
        cf.await()
    }

    public suspend fun handleAskDecision(changeId: String): Change =
        consensusProtocol.getState()
            .toEntryList()
            .find { it.getParentId() == changeId }
            ?.let { Change.fromHistoryEntry(it) }
            ?: throw ChangeDoesntExist(changeId)


    companion object {
        private val logger = LoggerFactory.getLogger("2PC")
    }

    override fun getLogger(): Logger = logger

    override fun getPeerResolver(): PeerResolver = peerResolver

    override fun putChangeToCompletableFutureMap(change: Change, completableFuture: CompletableFuture<ChangeResult>) {
        changeIdToCompletableFuture[change.toHistoryEntry().getId()] = completableFuture
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    private fun signal(signal: Signal, change: Change) {
        signalPublisher.signal(
            signal,
            this,
            getPeersFromChange(change),
            null,
            change
        )
    }

    private suspend fun askForDecisionChange(change: Change, iteration: Int = 0) {
        val resultChange = protocolClient.askForChangeStatus(
            change.peers.first { it != myAddress() },
            change
        )

        if (resultChange == null && iteration == twoPCConfig.maxChangeRetries)
            throw TwoPCConflictException("We are blocked, because we didn't received decision change")

        if (resultChange != null) handleDecision(resultChange)
        else changeTimer.startCounting {
            askForDecisionChange(change, iteration + 1)
        }
    }

    private fun checkChangeCompatibility(change: Change) =
        if (history.isEntryCompatible(change.toHistoryEntry()).not()) {
            logger.info("Change is not compatible with history $change")
            throw HistoryCannotBeBuildException()
        } else Unit

    private suspend fun checkChangeAndProposeToConsensus(change: Change): CompletableFuture<ChangeResult> = change
        .also { checkChangeCompatibility(it) }
        .let { consensusProtocol.proposeChangeAsync(change) }


    private suspend fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!
            .complete(ChangeResult(ChangeResult.Status.CONFLICT))
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