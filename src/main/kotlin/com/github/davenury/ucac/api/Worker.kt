package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.PeersetProtocols
import com.zopa.ktor.opentracing.span
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.concurrent.CompletableFuture


class Worker(
    private val coroutineScope: CoroutineScope,
    private val peersetId: PeersetId,
    private val peersetProtocols: PeersetProtocols,
    private val changeNotifier: ChangeNotifier,
) {
    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        peersetProtocols.consensusProtocol.getChangeResult(changeId)
            ?: peersetProtocols.gpacFactory.getChangeStatus(changeId)
            ?: peersetProtocols.twoPC.getChangeResult(changeId)
            ?: throw ChangeDoesntExist(changeId)

    fun startAsync() {
        MDC.putCloseable("peerset", peersetId.toString()).use {
            coroutineScope.launch(MDCContext()) {
                try {
                    run()
                } catch (e: Exception) {
                    logger.error("Error while running worker for $peersetId", e)
                    throw e
                }
            }
        }
    }

    private suspend fun run() {
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
            Thread.currentThread().interrupt()
        }
    }

    private suspend fun processQueueElement()  {
        val job = queue.receive()
        logger.info("Received a job: $job")
        Metrics.startTimer(job.change.id)
//        this.setTag("changeId", job.change.id)
        val result =
            when (job.protocolName) {
                ProtocolName.CONSENSUS -> peersetProtocols.consensusProtocol.proposeChangeAsync(job.change)
                ProtocolName.TWO_PC -> peersetProtocols.twoPC.proposeChangeAsync(job.change)
                ProtocolName.GPAC -> peersetProtocols.gpacFactory.getOrCreateGPAC(job.change.id)
                    .proposeChangeAsync(job.change)
            }
        result.thenAccept {
            logger.info("Job with changeId: ${job.change.id} finished with result $it")
            job.completableFuture.complete(it)
            Metrics.stopTimer(job.change.id, job.protocolName.name.lowercase(), it)
            Metrics.bumpChangeProcessed(it, job.protocolName.name.lowercase())
//            this.setTag("result", it.status.name.lowercase())
//            this.finish()
            changeNotifier.notify(job.change, it)
        }.await()
    }

    suspend fun send(job: ProcessorJob) {
        val peersetIds = job.change.peersets.map { it.peersetId }
        if (peersetId !in peersetIds) {
            throw AssertionError("Job is not related to my peerset: $job")
        }

        queue.send(job)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("worker")
    }
}
