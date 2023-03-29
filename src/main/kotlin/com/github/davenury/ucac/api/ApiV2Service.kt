package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Changes
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.PeersetProtocols
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val config: Config,
    private val peersetProtocols: PeersetProtocols,
) {
    private val worker = Worker(peersetProtocols)

    init {
        worker.startThread()
    }

    private fun history(): History {
        return peersetProtocols.history
    }

    fun getChanges(): Changes {
        return Changes.fromHistory(history())
    }

    fun getChangeById(id: String): Change? {
        return history().getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        worker.getChangeStatus(changeId)

    suspend fun addChange(job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
            logger.info("Service send job $job to queue")
            worker.send(it)
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
