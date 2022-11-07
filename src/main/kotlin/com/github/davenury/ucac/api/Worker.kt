package com.github.davenury.ucac.api

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.gpac.GPACProtocol
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantLock


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
                    if (job.gpacChange) gpacProtocol.proposeChangeAsync(job.change)
                    else consensusProtocol.proposeChangeAsync(job.change)
                job.completableFuture.complete(result.await())
            }
        } catch (e: Exception) {
            when (e) {
                is InterruptedException -> {
                    println("Worker interrupted")
                    processingQueue()
                }

                else -> processingQueue()
            }
        }
    }

    override fun run() = runBlocking {
        processingQueue()
    }

}

data class ProcessorJob(
    val change: Change,
    val completableFuture: CompletableFuture<ChangeResult>,
    val gpacChange: Boolean = true
)