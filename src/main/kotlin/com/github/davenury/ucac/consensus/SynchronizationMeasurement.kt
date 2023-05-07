package com.github.davenury.ucac.consensus

import com.github.davenury.common.Metrics
import com.github.davenury.common.PeerId
import com.github.davenury.common.history.History
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.coroutineContext

data class SynchronizationMeasurement(
    val history: History,
    val protocolClient: ConsensusProtocolClient,
    val consensusProtocol: ConsensusProtocol,
    val peerId: PeerId
) {
    private val mutex = Mutex()
    private var isSynchronized = false
    val startTime: Instant = Instant.now()
    private val currentEntryId: String = history.getCurrentEntryId()
    private var latestEntryId: String? = null
    private val entryIdToTime: ConcurrentHashMap<String, Instant> = ConcurrentHashMap()

    suspend fun begin(ctx: ExecutorCoroutineDispatcher) {
        withContext(ctx) {
            launch {
                var latestEntryId: String? = null
                var iter = 0
                while (latestEntryId == null && iter < 3) {
                    logger.info("Peers to which we send messages: ${consensusProtocol.otherConsensusPeers()}")
                    latestEntryId = getLatestEntryIdFromOtherPeers(currentEntryId)
                    iter += 1
                    delay(500)
                }

                mutex.withLock {
                    if (latestEntryId == currentEntryId) {
                        isSynchronized = true
                        clearMap()
                    } else if (latestEntryId == null) {
                        logger.info("Unable to get information about latest entry id, so assume we are synchronized")
                        isSynchronized = true
                        clearMap()
                    } else if (entryIdToTime.containsKey(latestEntryId)) {
                        isSynchronizationFinished(latestEntryId)
                    } else {
                        this@SynchronizationMeasurement.latestEntryId = latestEntryId
                    }
                }
            }
        }
    }

    public suspend fun isSynchronized(): Boolean = mutex.withLock { isSynchronized }

    private fun isSynchronizationFinished(entryId: String) = if (entryId == latestEntryId) {
        val timeElapsed = Duration.between(startTime, entryIdToTime[latestEntryId])
        Metrics.synchronizationTimer(peerId, timeElapsed)
        isSynchronized = true
        clearMap()
    } else {
    }

    private fun clearMap() = entryIdToTime.keys().toList().forEach { entryIdToTime.remove(it) }

    private suspend fun getLatestEntryIdFromOtherPeers(currentEntryId: String): String? =
        if (consensusProtocol.otherConsensusPeers().isEmpty()) currentEntryId
        else protocolClient
            .sendLatestEntryIdQuery(consensusProtocol.otherConsensusPeers(), currentEntryId)
            .mapNotNull { it.message }
            .maxByOrNull { it.distanceFromInitial }
            ?.entryId

    suspend fun entryIdCommitted(entryId: String, instant: Instant) =
        if (!isSynchronized) {
            mutex.withLock {
                if (isSynchronized) return@withLock
                entryIdToTime[entryId] = instant
                isSynchronizationFinished(entryId)
            }
        } else {
        }

    companion object {
        val logger = LoggerFactory.getLogger("consensus-synchronization-measuremenet")
    }
}