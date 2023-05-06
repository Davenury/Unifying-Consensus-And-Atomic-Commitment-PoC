package com.github.davenury.ucac.consensus

import com.github.davenury.common.*
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerId
import com.github.davenury.common.history.History
import com.github.davenury.common.txblocker.PersistentTransactionBlocker
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.consensus.alvin.AlvinProtocol
import com.github.davenury.ucac.consensus.alvin.AlvinProtocolClientImpl
import com.github.davenury.ucac.consensus.paxos.PigPaxosProtocolClientImpl
import com.github.davenury.ucac.consensus.paxos.PaxosProtocolImpl
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocolImpl
import com.github.davenury.ucac.consensus.raft.RaftProtocolClientImpl
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import java.util.concurrent.CompletableFuture

//Add tests for PigPaxos and Alvin where for some time there are no quorum and then one peer starts responding to verify that it is possible to synchronize state


interface ConsensusProtocol {
    suspend fun begin()
    fun stop()
    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change)

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    fun isMoreThanHalf(value: Int): Boolean = (value + 1) * 2 > otherConsensusPeers().size + 1


    fun otherConsensusPeers(): List<PeerAddress>

    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>

    companion object {
        fun createConsensusProtocol(
            config: Config,
            peersetId: PeersetId,
            history: History,
            ctx: ExecutorCoroutineDispatcher,
            transactionBlocker: PersistentTransactionBlocker,
            peerResolver: PeerResolver,
            signalPublisher: SignalPublisher,
            subscribers: Subscribers?,
        ): ConsensusProtocol = when (config.consensus.name) {

            "raft" -> RaftConsensusProtocolImpl(
                peersetId,
                history,
                config,
                ctx,
                peerResolver,
                signalPublisher,
                RaftProtocolClientImpl(peersetId),
                transactionBlocker = transactionBlocker,
                subscribers
            )

            "alvin" -> AlvinProtocol(
                peersetId,
                history,
                ctx,
                peerResolver,
                signalPublisher,
                AlvinProtocolClientImpl(peersetId),
                heartbeatTimeout = config.consensus.heartbeatTimeout,
                heartbeatDelay = config.consensus.leaderTimeout,
                transactionBlocker = transactionBlocker,
                config.metricTest,
                subscribers,
                maxChangesPerMessage = config.consensus.maxChangesPerMessage
            )

            "paxos" -> PaxosProtocolImpl(
                peersetId,
                history,
                ctx,
                peerResolver,
                signalPublisher,
                PigPaxosProtocolClientImpl(peersetId),
                heartbeatTimeout = config.consensus.heartbeatTimeout,
                heartbeatDelay = config.consensus.leaderTimeout,
                transactionBlocker = transactionBlocker,
                config.metricTest,
                subscribers,
                maxChangesPerMessage = config.consensus.maxChangesPerMessage
            )

            else -> throw IllegalStateException("Unknow consensus type ${config.consensus.name}")
        }
    }


    fun amILeader(): Boolean
    fun getLeaderId(): PeerId?
}

typealias ConsensusProposeChange = Change

data class VotedFor(val id: PeerId, val elected: Boolean = false)
