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
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
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
    val history = MeteredHistory(PersistentHistory(persistence))
    private val transactionBlocker = PersistentTransactionBlocker(persistence)

    private val ctx: ExecutorCoroutineDispatcher =
        Executors.newCachedThreadPool().asCoroutineDispatcher()

    val consensusProtocol: RaftConsensusProtocol
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

        consensusProtocol = RaftConsensusProtocolImpl(
            peersetId,
            history,
            config,
            ctx,
            peerResolver,
            signalPublisher,
            RaftProtocolClientImpl(peersetId),
            transactionBlocker,
        )

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
