package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import com.zopa.ktor.opentracing.span
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import okhttp3.internal.notifyAll
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

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
    val currentConsensusLeaders = ConcurrentHashMap<PeersetId, PeerAddress>()

    override suspend fun performProtocol(change: Change): Unit = span("TwoPC.performProtocol") {
        this.setTag("changeId", change.id)
        val updatedChange =
            updateParentIdFor2PCCompatibility(change, history, peersetId)

        val mainChangeId = updatedChange.id
        try {
            val acceptChange = TwoPCChange(
                peersets = updatedChange.peersets,
                twoPCStatus = TwoPCStatus.ACCEPTED,
                change = change,
                leaderPeerset = peersetId,
            )

            val otherPeersets = updatedChange.peersets
                .map { it.peersetId }
                .filter { it != peersetId }
            val otherPeers: Map<PeersetId, PeerAddress> =
                otherPeersets.associateWith { currentConsensusLeaders[it] ?: peerResolver.getPeersFromPeerset(it)[0] }

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
            val decisionPhaseOtherPeers = otherPeersets.associateWith { currentConsensusLeaders[it] ?: peerResolver.getPeersFromPeerset(it)[0] }
            decisionPhase(acceptChange, decision, decisionPhaseOtherPeers)

            val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.ABORTED

            this.setTag("result", result.name.lowercase())
            this.finish()

            postDecisionOperations(mainChangeId, change, result)
        } catch (e: Exception) {
            changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            this.setTag("result", "conflict")
            this.finish()
        }
    }

    private fun postDecisionOperations(
        mainChangeId: String,
        change: Change,
        result: ChangeResult.Status
    ) {
        if (isMetricTest) {
            Metrics.bumpChangeMetric(
                changeId = mainChangeId,
                peerId = peerId,
                peersetId = peersetId,
                protocolName = ProtocolName.TWO_PC,
                state = result.name.lowercase()
            )
        }

        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(result))
        signal(Signal.TwoPCOnChangeApplied, change)
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
        val result = if (consensusProtocol.amILeader()) {
            consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()
        } else {
            consensusProtocol.getLeaderId()?.let {
                throw ImNotLeaderException(it, peersetId)
            } ?: kotlin.run {
                logger.error("There's no consensus leader!")
                throw TwoPCHandleException("TwoPCChange didn't apply change")
            }
        }

        if (result.status != ChangeResult.Status.SUCCESS) {
            logger.error("Two pc did not apply change, result is ${result.status.name.lowercase()}")
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    private suspend fun askForDecisionChange(change: Change): Unit = span("TwoPc.askForDecisionChange") {
        val otherPeerset = change.peersets.map { it.peersetId }
            .first { it != peersetId }

        signal(Signal.TwoPCOnAskForDecision, change)
        val resultChange = protocolClient.askForChangeStatus(
            otherPeerset.let { peerResolver.getPeersFromPeerset(it)[0] },
            change,
            otherPeerset,
        )

        logger.info("Asking about change: $change - result - $resultChange")
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
                    val updatedChange = change.copyWithNewParentId(
                        peersetId,
                        history.getCurrentEntryId(),
                    )
                    checkChangeAndProposeToConsensus(updatedChange, currentProcessedChange.change.id)
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
            signal(Signal.TwoPCOnHandleDecisionEnd, change)
        } catch (e: Exception) {
            changeConflict(mainChangeId, "Change conflicted in decision phase, ${e.message}")
            throw e
        }

        changeNotifier.notify(change, ChangeResult(ChangeResult.Status.SUCCESS))
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        changeIdToCompletableFuture[change.id]!!
            .complete(ChangeResult(ChangeResult.Status.SUCCESS))
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

    // I have been selected as a new leader, let me check if there are any 2PC changes
    fun newConsensusLeaderElected(peerId: PeerId, peersetId: PeersetId) {
        logger.info("I have been selected as a new consensus leader")
        val currentChange = Change.fromHistoryEntry(history.getCurrentEntry())
        logger.info("Current change in history is - $currentChange")
        if (currentChange !is TwoPCChange || currentChange.twoPCStatus == TwoPCStatus.ABORTED) {
            // everything good - current change is not 2PC change
            logger.info("There's no unfinished TwoPC changes, I can receive new changes")
            return
        }
        // try to find out what happened to the transaction
        if (currentChange.leaderPeerset == peersetId) {
            logger.info("Change $currentChange is my change, so I need to go with decision phase")
            // it was my change, so it's better to finish it
            runBlocking {
                // TODO - it does not have to be false - it was a fault so it's safer to abort the transaction
                decisionPhase(
                    currentChange,
                    false,
                    currentChange.peersets.filterNot { it.peersetId == peersetId }
                        .associate { it.peersetId to peerResolver.getPeersFromPeerset(it.peersetId)[0] })

                postDecisionOperations(
                    updateParentIdFor2PCCompatibility(currentChange.change, history, peersetId).id,
                    currentChange.change,
                    ChangeResult.Status.ABORTED,
                )
            }
        } else {
            logger.info("Change $currentChange is not my change, I'll ask about it")
            // it was not my change, so I should ask about it
            runBlocking {
                askForDecisionChange(currentChange)
            }
        }
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    private suspend fun proposePhase(
        acceptChange: TwoPCChange,
        mainChangeId: String,
        otherPeers: Map<PeersetId, PeerAddress>,
    ): Boolean = span("TwoPc.proposePhase") {
        val acceptResult = checkChangeAndProposeToConsensus(acceptChange, mainChangeId).await()

        if (acceptResult.status != ChangeResult.Status.SUCCESS) {
            changeConflict(mainChangeId, "failed during processing acceptChange in 2PC")
        } else {
            logger.info("Change accepted locally ${acceptChange.change}")
        }

        val decision = getProposePhaseResponses(otherPeers, acceptChange, mapOf())

        logger.info("Decision $decision from other peerset for ${acceptChange.change}")

        return decision
    }

    suspend fun getProposePhaseResponses(
        peers: Map<PeersetId, PeerAddress>,
        change: Change,
        recentResponses: Map<PeerAddress, TwoPCRequestResponse>
    ): Boolean {
        val responses = protocolClient.sendAccept(peers, change)

        val addressesToAskAgain = responses.filter { (_, response) -> response.redirect }
            .map { (_, response) ->
                val address = peerResolver.resolve(response.newConsensusLeaderId!!)
                logger.debug(
                    "Updating {} peerset to new consensus leader: {}",
                    response.newConsensusLeaderPeersetId,
                    response.newConsensusLeaderId
                )
                currentConsensusLeaders[response.newConsensusLeaderPeersetId!!] = address
                response.peersetId to address
            }.toMap()

        val deadPeersSubstitute = responses.filter { (_, response) -> response.failureBecauseOfDeadPeer }
            .map { (peerAddress, response) ->
                val peers = peerResolver.getPeersFromPeerset(response.peersetId)
                val addressIndex = peers.indexOf(peerAddress)
                if (addressIndex == -1) {
                    throw IllegalStateException("Peer $peerAddress not found in peerset ${response.peersetId}")
                }
                val newAddress = peers.getOrNull((addressIndex + 1) % peers.size)
                    ?: throw java.lang.IllegalStateException("I do not have any alive peer to ask in peerset ${response.peersetId}")
                currentConsensusLeaders[response.peersetId] = newAddress
                logger.info("Peer from peerset ${response.peersetId} appears to be dead, I get next peer ${newAddress.peerId}")
                response.peersetId to newAddress
            }

        if (addressesToAskAgain.isEmpty() && deadPeersSubstitute.isEmpty()) {
            return (recentResponses + responses).values.map { it.success }.all { it }
        }

        return getProposePhaseResponses(
            addressesToAskAgain + deadPeersSubstitute,
            change,
            (recentResponses + responses.filter { it.value.success })
        )
    }

    private suspend fun decisionPhase(
        acceptChange: TwoPCChange,
        decision: Boolean,
        otherPeers: Map<PeersetId, PeerAddress>,
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
                leaderPeerset = peersetId,
            )
        }
        logger.info("Change to commit: $commitChange")

        protocolClient.sendDecision(otherPeers, commitChange)
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
            changeIdToCompletableFuture[originalChangeId]!!.complete(ChangeResult(ChangeResult.Status.REJECTED, currentEntryId = history.getCurrentEntryId()))
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(
        change: Change,
        originalChangeId: String,
    ): CompletableFuture<ChangeResult> = change
        .also { checkChangeCompatibility(it, originalChangeId) }
        .let { consensusProtocol.proposeChangeAsync(change) }


    private fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture
            .also { it.putIfAbsent(changeId, CompletableFuture()) }
            .get(changeId)!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
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