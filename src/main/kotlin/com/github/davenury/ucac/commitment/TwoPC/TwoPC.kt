package com.github.davenury.ucac.commitment.TwoPC

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.commitment.AtomicCommitmentProtocol
import com.github.davenury.ucac.commitment.gpac.GPACProtocolImpl
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

class TwoPC(
    private val history: History,
    private val gpacConfig: GpacConfig,
    private val protocolClient: TwoPCProtocolClient,
    private val consensusProtocol: ConsensusProtocol,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val myPeersetId: Int,
    private val myNodeId: Int,
    private val peerResolver: PeerResolver
) : SignalSubject, AtomicCommitmentProtocol {

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    override fun getPeerName() = "peerset${myPeersetId}/peer${myNodeId}"

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()


        val enrichedChange =
            if (change.peers.contains(myAddress())) {
                change
            } else {
                change.withAddress(myAddress())
            }
        changeIdToCompletableFuture[enrichedChange.toHistoryEntry().getId()] = cf

        GlobalScope.launch {
            performProtocol(enrichedChange)
        }

        return cf
    }


    private suspend fun performProtocol(change: Change) {

        val mainChangeId = change.toHistoryEntry().getId()

        val acceptChange = TwoPCChange(change.parentId, TwoPCStatus.ACCEPTED, change.peers, change = change)

        val acceptResult = checkChangeAndProposeToConsensus(acceptChange)

        if (acceptResult.status != ChangeResult.Status.SUCCESS) changeConflict(
            mainChangeId,
            "failed during processing acceptChange"
        )

        val otherPeersets = change.peers.filter { it != myAddress() }
        val decision = protocolClient
            .sendAccept(otherPeersets, acceptChange)
            .all { it }

        val acceptChangeId = acceptChange.toHistoryEntry().getId()

        val commitChange = if (decision) change.copyWithNewParentId(acceptChangeId)
        else TwoPCChange.fromChange(change, TwoPCStatus.ABORTED, acceptChangeId)

        commitChange(commitChange, otherPeersets)

        changeIdToCompletableFuture[change.toHistoryEntry()
            .getId()]!!.complete(ChangeResult(ChangeResult.Status.SUCCESS))
    }

    private suspend fun commitChange(change: Change, otherPeersets: List<String>) {
//      We are in critical section
        consensusProtocol.proposeChangeAsync(change)
        val commitResult = protocolClient.sendCommitChange(otherPeersets, change)
        if (commitResult.any { it.not() }) throw Exception("Some peer doesn't applied commitChange we are wasted")
    }

    public suspend fun handleAccept(change: Change) {
        checkChangeCompatibility(change)
        consensusProtocol.proposeChangeAsync(change).await()
    }

    public suspend fun handleCommit(change: Change) {
        checkChangeCompatibility(change)
        consensusProtocol.proposeChangeAsync(change).await()
    }

    public suspend fun handleDecision(change: Change) {
        when {
            change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ACCEPTED ->
                throw Exception("In 2PC handleDecision received change, which accepted something")

            change is TwoPCChange -> checkChangeAndProposeToConsensus(change)

            else -> throw Exception("In 2PC handleDecision received change in different type than TwoPCChange")
        }
    }

    private fun checkChangeCompatibility(change: Change) =
        if (history.isEntryCompatible(change.toHistoryEntry()).not()) throw HistoryCannotBeBuildException()
        else Unit

    private suspend fun checkChangeAndProposeToConsensus(change: Change): ChangeResult = change
        .also { checkChangeCompatibility(it) }
        .let { consensusProtocol.proposeChangeAsync(change).await() }


    private suspend fun changeConflict(changeId: String, exceptionText: String) =
        changeIdToCompletableFuture[changeId]!!
            .complete(ChangeResult(ChangeResult.Status.CONFLICT))
            .also { throw TwoPCConflictException(exceptionText) }


    private fun myAddress() = peerResolver.currentPeerAddress().address

    companion object {
        private val logger = LoggerFactory.getLogger(TwoPC::class.java)
    }
}