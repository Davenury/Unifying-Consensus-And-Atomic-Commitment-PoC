package com.github.davenury.ucac.api

import com.github.davenury.common.Metrics
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.meterRegistry
import com.github.davenury.ucac.commitment.gpac.GPACProtocolAbstract
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.consensus.ConsensusProtocol
import io.micrometer.core.instrument.LongTaskTimer
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.*


class Worker(
    private val queue: Channel<ProcessorJob>,
    private val gpacProtocol: GPACProtocolAbstract,
    private val consensusProtocol: ConsensusProtocol,
    private val twoPC: TwoPC,
    passMdc: Boolean = true
) : Runnable {
    private var mdc: MutableMap<String, String>? = if (passMdc) {
        MDC.getCopyOfContextMap()
    } else {
        null
    }

    private suspend fun processingQueue() {
        try {
            while (!Thread.interrupted()) {
                val job = queue.receive()
                logger.info("Worker receive job: $job")

                Metrics.startTimer(job.change.id, job.protocolName.name.lowercase())

                val result =
                    when (job.protocolName) {
                        ProtocolName.CONSENSUS -> consensusProtocol.proposeChangeAsync(job.change)
                        ProtocolName.TWO_PC -> twoPC.proposeChangeAsync(job.change)
                        ProtocolName.GPAC -> gpacProtocol.proposeChangeAsync(job.change)
                    }
                result.thenAccept {
                    job.completableFuture.complete(it)
                    Metrics.stopTimer(job.change.id)
                    runBlocking {
                        ChangeNotifier.notify(job.change, it)
                    }
                }.await()
            }
        } catch (e: Exception) {
            if (e is InterruptedException) {
                logger.debug("Worker interrupted")
            }
            processingQueue()
        }
    }

    override fun run() = runBlocking {
        mdc?.let { MDC.setContextMap(it) }
        processingQueue()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("worker")
    }
}

