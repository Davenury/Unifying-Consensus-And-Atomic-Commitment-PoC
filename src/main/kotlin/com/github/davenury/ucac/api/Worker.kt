package com.github.davenury.ucac.api

import com.github.davenury.ucac.commitment.gpac.GPACProtocol
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.infrastructure.RaftConsensusProtocolImpl
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CompletableFuture


class Worker(
    private val queue: Deque<ProcessorJob>,
    private val channel: Channel<Unit>,
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
) : Runnable {


    private suspend fun processingQueue() {
        try {
            while (!Thread.interrupted()) {
                while (queue.isEmpty()) channel.receive()
                val job = queue.pop()
                val result =
                    if (job.isConsensusOnly) consensusProtocol.proposeChangeAsync(job.change)
                    else gpacProtocol.proposeChangeAsync(job.change)
                job.completableFuture.complete(result.await())
            }
        } catch (e: Exception) {
            if (e is InterruptedException) {
                logger.debug("Worker interrupted")
            }
            processingQueue()
        }
    }

    override fun run() = runBlocking {
        processingQueue()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
    }

}

data class ProcessorJob(
    val change: Change,
    val completableFuture: CompletableFuture<ChangeResult>,
    val isConsensusOnly: Boolean = false
)