package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val gpacFactory: GPACFactory,
    private val consensusProtocol: ConsensusProtocol,
    private val twoPC: TwoPC,
    private val history: History,
    private var config: Config,
) {

    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)
    private val worker = Worker(queue, gpacFactory, consensusProtocol, twoPC)
    private val workerThread: Thread = Thread(worker)

    init {
        workerThread.start()
    }

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)
                ?.let { Transition.fromHistoryEntry(it) }
                ?.let { it as? ChangeApplyingTransition }?.change
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        worker.getChangeStatus(changeId)

    suspend fun addJob(job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
            logger.info("Service send job $job to queue")
            queue.send(it)
        }.completableFuture

    suspend fun addJobSync(
        job: ProcessorJob,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addJob(job).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger("ApiV2Service")
    }
}
