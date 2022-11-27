package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.commitment.TwoPC.TwoPC
import com.github.davenury.ucac.commitment.gpac.GPACProtocol
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import io.ktor.client.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.*
import java.util.concurrent.CompletableFuture


class Worker(
    private val queue: Channel<ProcessorJob>,
    private val gpacProtocol: GPACProtocol,
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
                val result =
                    when (job.processorJobType) {
                        ProcessorJobType.CONSENSUS -> consensusProtocol.proposeChangeAsync(job.change)
                        ProcessorJobType.TWO_PC -> twoPC.proposeChangeAsync(job.change)
                        ProcessorJobType.GPAC -> gpacProtocol.proposeChangeAsync(job.change)
                    }
                result.thenAccept { job.completableFuture.complete(it) }
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

