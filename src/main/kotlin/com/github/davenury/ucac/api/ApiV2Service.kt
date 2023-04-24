package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Changes
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

class ApiV2Service(
    private val config: Config,
    private val multiplePeersetProtocols: MultiplePeersetProtocols,
    changeNotifier: ChangeNotifier,
) {
    private val workerPool = Executors.newFixedThreadPool(config.workerPoolSize)
        .asCoroutineDispatcher()
    private val workerCoroutineScope = CoroutineScope(workerPool)

    private val workers: Map<PeersetId, Worker> =
        multiplePeersetProtocols.protocols.mapValues { (peersetId, protocols) ->
            Worker(workerCoroutineScope, peersetId, protocols, changeNotifier)
        }

    init {
        workers.values.forEach { it.startAsync() }
    }

    private fun history(peersetId: PeersetId): History {
        return multiplePeersetProtocols.forPeerset(peersetId).history
    }

    fun getChanges(peersetId: PeersetId): Changes {
        return Changes.fromHistory(history(peersetId))
    }

    fun getLastChange(peersetId: PeersetId): Change? =
        Change.fromHistoryEntry(history(peersetId).getCurrentEntry())

    fun getChangeById(peersetId: PeersetId, id: String): Change? =
        history(peersetId).getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }

    fun getChangeStatus(peersetId: PeersetId, changeId: String): CompletableFuture<ChangeResult> =
        workers[peersetId]!!.getChangeStatus(changeId)

    suspend fun addChange(peersetId: PeersetId, job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
            logger.info("Service send job $job to queue")
            workers[peersetId]!!.send(it)
        }.completableFuture

    suspend fun addChangeSync(
        peersetId: PeersetId,
        job: ProcessorJob,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addChange(peersetId, job).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger("ApiV2Service")
    }
}
