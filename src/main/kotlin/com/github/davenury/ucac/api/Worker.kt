package com.github.davenury.ucac.api

import com.github.davenury.common.ChangeDoesntExist
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Metrics
import com.github.davenury.common.ProtocolName
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.PeersetProtocols
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.concurrent.CompletableFuture


class Worker(
    private val peersetProtocols: PeersetProtocols,
) : Runnable {
    private var mdc: MutableMap<String, String> = MDC.getCopyOfContextMap()
    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        peersetProtocols.consensusProtocol.getChangeResult(changeId)
            ?: peersetProtocols.gpacFactory.getChangeStatus(changeId)
            ?: peersetProtocols.twoPC.getChangeResult(changeId)
            ?: throw ChangeDoesntExist(changeId)

    fun startThread() {
        Thread(this).start()
    }

    override fun run() = runBlocking {
        MDC.setContextMap(mdc)
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
                ProtocolName.CONSENSUS -> peersetProtocols.consensusProtocol.proposeChangeAsync(job.change)
                ProtocolName.TWO_PC -> peersetProtocols.twoPC.proposeChangeAsync(job.change)
                ProtocolName.GPAC -> peersetProtocols.gpacFactory.getOrCreateGPAC(job.change.id)
                    .proposeChangeAsync(job.change)
            }
        result.thenAccept {
            job.completableFuture.complete(it)
            Metrics.stopTimer(job.change.id, job.protocolName.name.lowercase(), it)
            Metrics.bumpChangeProcessed(it, job.protocolName.name.lowercase())
            ChangeNotifier.notify(job.change, it)
        }.await()
    }

    suspend fun send(job: ProcessorJob) {
        queue.send(job)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("worker")
    }
}
