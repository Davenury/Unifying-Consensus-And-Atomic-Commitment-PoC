package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.structure.HttpSubscriber
import com.github.davenury.ucac.consensus.LatestEntryIdResponse
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
    private val peerResolver: PeerResolver,
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

    fun registerSubscriber(peersetId: PeersetId, address: SubscriberAddress) {
        multiplePeersetProtocols.registerSubscriber(peersetId, HttpSubscriber(address.address))
    }

    fun getLastChange(peersetId: PeersetId): Change? =
        Change.fromHistoryEntry(history(peersetId).getCurrentEntry())

    fun getChangeById(peersetId: PeersetId, id: String): Change? =
        history(peersetId).getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }

    fun getChangeStatus(peersetId: PeersetId, changeId: String): CompletableFuture<ChangeResult> =
        workers[peersetId]!!.getChangeStatus(changeId)

    suspend fun addChange(peersetId: PeersetId, job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
//            logger.info("Service send job $job to queue")
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

    fun getPeersetInformation(peersetId: PeersetId): PeersetInformation {
        return PeersetInformation(
            currentConsensusLeader = multiplePeersetProtocols.protocols[peersetId]?.consensusProtocol?.getLeaderId(),
            peersInPeerset = peerResolver.getPeersFromPeerset(peersetId).map { it.peerId }
        )
    }

    fun getLatestEntryIdResponse(entryId: String,peersetId: PeersetId): LatestEntryIdResponse{
        val consensusProtocol = multiplePeersetProtocols.protocols[peersetId]?.consensusProtocol!!
        val entries = consensusProtocol.getState().getAllEntriesUntilHistoryEntryId(entryId)
        return LatestEntryIdResponse(
            consensusProtocol.getState().getCurrentEntryId(),
            entries.size
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger("ApiV2Service")
    }
}
