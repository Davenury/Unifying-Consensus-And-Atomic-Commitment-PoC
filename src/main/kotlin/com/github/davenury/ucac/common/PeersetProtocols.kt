package com.github.davenury.ucac.common

import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.commitment.twopc.TwoPCProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.RaftProtocolClientImpl
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import kotlinx.coroutines.*
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
class PeersetProtocols(
    val peersetId: PeersetId,
    val config: Config,
    val peerResolver: PeerResolver,
    signalPublisher: SignalPublisher,
    changeNotifier: ChangeNotifier
) : AutoCloseable {
    val history = HistoryFactory().createForConfig(config)
    private val transactionBlocker = TransactionBlocker()

    private val ctx: ExecutorCoroutineDispatcher =
        Executors.newCachedThreadPool().asCoroutineDispatcher()

    val consensusProtocol: RaftConsensusProtocol
    val twoPC: TwoPC
    val gpacFactory: GPACFactory

    fun beginConsensusProtocol() {
        ctx.dispatch(Dispatchers.IO) {
            runBlocking {
                if (config.raft.isEnabled) {
                    consensusProtocol.begin()
                }
            }
        }
    }

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
            RaftProtocolClientImpl(),
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
