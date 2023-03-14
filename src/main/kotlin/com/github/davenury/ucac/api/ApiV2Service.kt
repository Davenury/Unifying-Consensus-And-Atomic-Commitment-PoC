package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Changes
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.common.PeerResolver
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    gpacFactory: GPACFactory,
    consensusProtocol: ConsensusProtocol,
    twoPC: TwoPC,
    private val history: History,
    private var config: Config,
    peerResolver: PeerResolver,
) {

    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)
    private val worker = Worker(queue, gpacFactory, consensusProtocol, twoPC, peerResolver)
    private val workerThread: Thread = Thread(worker)

    init {
        workerThread.start()
    }

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getLastChange(): Change? {
        return Change.fromHistoryEntry(history.getCurrentEntry())
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        worker.getChangeStatus(changeId)

    suspend fun addChange(job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
            logger.info("Service send job $job to queue")
            queue.send(it)
        }.completableFuture

    suspend fun addChangeSync(
        job: ProcessorJob,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addChange(job).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger("ApiV2Service")
    }
}
