package com.github.davenury.ucac.api

import com.github.davenury.common.ChangeDoesntExist
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Metrics
import com.github.davenury.common.ProtocolName
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.common.PeerResolver
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.concurrent.CompletableFuture


class Worker(
    private val queue: Channel<ProcessorJob>,
    private val gpacFactory: GPACFactory,
    private val consensusProtocol: ConsensusProtocol,
    private val twoPC: TwoPC,
    private val peerResolver: PeerResolver,
    passMdc: Boolean = true
) : Runnable {
    private var mdc: MutableMap<String, String>? = if (passMdc) {
        MDC.getCopyOfContextMap()
    } else {
        null
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        consensusProtocol.getChangeResult(changeId)
            ?: gpacFactory.getChangeStatus(changeId)
            ?: twoPC.getChangeResult(changeId)
            ?: throw ChangeDoesntExist(changeId)

    override fun run() = runBlocking {
        mdc?.let { MDC.setContextMap(it) }
        processQueue()
    }

    private suspend fun processQueue() {
        try {
            while (!Thread.interrupted()) {
                try {
                    processQueueElement()
                } catch (e: Exception) {
                    logger.error("Error while processing queue, ignoring", e)
                }
            }
        } catch (e: InterruptedException) {
            logger.info("Worker interrupted")
        }
    }

    private suspend fun processQueueElement() {
        val job = queue.receive()
        logger.info("Received a job: $job")
        Metrics.startTimer(job.change.id)
        val result =
            when (job.protocolName) {
                ProtocolName.CONSENSUS -> consensusProtocol.proposeChangeAsync(job.change)
                ProtocolName.TWO_PC -> twoPC.proposeChangeAsync(job.change)
                ProtocolName.GPAC -> gpacFactory.getOrCreateGPAC(job.change.id)
                    .proposeChangeAsync(job.change)
            }
        result.thenAccept {
            job.completableFuture.complete(it)
            Metrics.stopTimer(job.change.id, job.protocolName.name.lowercase(), it)
            Metrics.bumpChangeProcessed(it, job.protocolName.name.lowercase())
            ChangeNotifier.get(peerResolver = peerResolver).notify(job.change, it)
        }.await()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("worker")
    }
}
