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
) : SignalSubject, AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)

    override suspend fun performProtocol(change: Change) {
        val mainChangeId = change.id

        val acceptChange = TwoPCChange(
            peersets = change.peersets,
            twoPCStatus = TwoPCStatus.ACCEPTED,
            change = change,
        )

        val otherPeers = change.peersets
            .map { it.peersetId }
            .filter { it != peerResolver.currentPeer().peersetId }
            .map { peerResolver.resolve(GlobalPeerId(it, 0)) }

        val decision = proposePhase(acceptChange, mainChangeId, otherPeers)
        decisionPhase(acceptChange, decision, otherPeers)

        val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.CONFLICT
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(result))
    }

    suspend fun handleAccept(change: Change) {
        if (change !is TwoPCChange) {
            logger.info("Received not 2PC change $change")
            throw TwoPCHandleException("Received change of not TwoPCChange in handleAccept: $change")
        }

        val changeWithProperParentId = change.copyWithNewParentId(
            peerResolver.currentPeer().peersetId,
            history.getCurrentEntry().getId(),
        )
        val result = consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()

        if (result.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    private suspend fun askForDecisionChange(change: Change) {
        val resultChange = protocolClient.askForChangeStatus(
            change.peersets.map { it.peersetId }
                .first { it != peerResolver.currentPeer().peersetId }
                .let { peerResolver.resolve(GlobalPeerId(it, 0)) },
            change
        )

        if (resultChange != null) {
            handleDecision(resultChange)
        } else changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    suspend fun handleDecision(change: Change) {
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
                val updatedChange = change.copyWithNewParentId(
                    peerResolver.currentPeer().peersetId,
                    history.getCurrentEntry().getId(),
                )
                checkChangeAndProposeToConsensus(updatedChange)
            }

            else -> throw TwoPCHandleException(
                "In 2PC handleDecision received change in different type than TwoPCChange: $change \n" +
                        "currentProcessedChange: $currentProcessedChange"
            )
        }
        cf.await()
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
    ): Boolean {
        val acceptResult = checkChangeAndProposeToConsensus(acceptChange).await()

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
    ) {

        val change = acceptChange.change
        val acceptChangeId = acceptChange.toHistoryEntry(peerResolver.currentPeer().peersetId).getId()

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
                peerResolver.currentPeer().peersetId,
                acceptChangeId,
            )
        ).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCConflictException("Change failed during committing locally")
        }

        logger.info("Decision $decision committed in all peersets $commitChange")
        signal(Signal.TwoPCOnChangeApplied, change)
    }

    private fun signal(signal: Signal, change: Change) {
        signalPublisher.signal(
            signal, this, getPeersFromChange(change), null, change
        )
    }

    private fun checkChangeCompatibility(change: Change) {
        if (!history.isEntryCompatible(change.toHistoryEntry(peerResolver.currentPeer().peersetId))) {
            logger.info("Change is not compatible with history $change")
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(change: Change): CompletableFuture<ChangeResult> = change
        .also { checkChangeCompatibility(it) }
        .let { consensusProtocol.proposeChangeAsync(change) }

    private fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            .also { throw TwoPCConflictException(exceptionText) }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc")
    }
}
