package com.github.davenury.ucac.common

import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.MeteredHistory
import com.github.davenury.common.history.PersistentHistory
import com.github.davenury.common.txblocker.PersistentTransactionBlocker
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.commitment.twopc.TwoPCProtocolClientImpl
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.alvin.AlvinProtocol
import com.github.davenury.ucac.consensus.alvin.AlvinProtocolClientImpl
import com.github.davenury.ucac.consensus.pigpaxos.PigPaxosProtocol
import com.github.davenury.ucac.consensus.pigpaxos.PigPaxosProtocolClientImpl
import com.github.davenury.ucac.consensus.pigpaxos.PigPaxosProtocolImpl
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocolImpl
import com.github.davenury.ucac.consensus.raft.RaftProtocolClientImpl
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
class PeersetProtocols(
    val peersetId: PeersetId,
    config: Config,
    val peerResolver: PeerResolver,
    signalPublisher: SignalPublisher,
    changeNotifier: ChangeNotifier,
) : AutoCloseable {
    private val persistence = PersistenceFactory().createForConfig(config)
    val history = PersistentHistory(persistence)
    private val transactionBlocker = PersistentTransactionBlocker(persistence)

    private val ctx: ExecutorCoroutineDispatcher =
        Executors.newCachedThreadPool().asCoroutineDispatcher()

    val consensusProtocol: ConsensusProtocol
    val twoPC: TwoPC
    val gpacFactory: GPACFactory

    init {
        gpacFactory = GPACFactory(
            peersetId,
            transactionBlocker,
            history,
            config,
            ctx,
            signalPublisher,
            peerResolver,
        )

        consensusProtocol =  when (config.consensus.name) {

            "raft" -> RaftConsensusProtocolImpl(
                peersetId,
                history,
                config,
                ctx,
                peerResolver,
                signalPublisher,
                RaftProtocolClientImpl(peersetId),
                transactionBlocker = transactionBlocker,
            )

            "alvin" -> AlvinProtocol(
                peersetId,
                history,
                ctx,
                peerResolver,
                signalPublisher,
                AlvinProtocolClientImpl(),
                heartbeatTimeout = config.consensus.heartbeatTimeout,
                heartbeatDelay = config.consensus.leaderTimeout,
                transactionBlocker = transactionBlocker,
                config.metricTest,
            )

            "pigpaxos" -> PigPaxosProtocolImpl(
                peersetId,
                history,
                ctx,
                peerResolver,
                signalPublisher,
                PigPaxosProtocolClientImpl(),
                heartbeatTimeout = config.consensus.heartbeatTimeout,
                heartbeatDelay = config.consensus.leaderTimeout,
                transactionBlocker = transactionBlocker,
                config.metricTest,
            )

            else -> throw RuntimeException("Unknow consensus type ${config.consensus.name}")
        }

        twoPC = TwoPC(
            peersetId,
            history,
            config.twoPC,
            ctx,
            TwoPCProtocolClientImpl(),
            consensusProtocol,
            peerResolver,
            signalPublisher,
            config.metricTest,
            changeNotifier = changeNotifier
        )
    }

    override fun close() {
        consensusProtocol.stop()
        ctx.close()
    }
}
