package com.github.davenury.ucac.consensus

import com.github.davenury.common.Metrics
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.history.History
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

data class SynchronizationMeasurement(
    val history: History,
    val protocolClient: ConsensusProtocolClient,
    val otherConsensusPeers: () -> List<PeerAddress>,
    val peerId: PeerId
) {
    private val mutex = Mutex()
    private var isSynchronized = false
    val startTime: Instant = Instant.now()
    private val currentEntryId: String = history.getCurrentEntryId()
    private var latestEntryId: String? = null
    private val entryIdToTime: ConcurrentHashMap<String, Instant> = ConcurrentHashMap()

    suspend fun begin() {
        val latestEntryId = getLatestEntryIdFromOtherPeers(currentEntryId)
        mutex.withLock {
            if (latestEntryId == null || latestEntryId == currentEntryId) {
                isSynchronized = true
                clearMap()
            } else if (entryIdToTime.containsKey(latestEntryId)) {
                isSynchronizationFinished(latestEntryId)
            } else {
                this.latestEntryId = latestEntryId
            }
        }
    }

    private fun isSynchronizationFinished(entryId: String) = if (entryId == latestEntryId) {
        val timeElapsed = Duration.between(startTime, entryIdToTime[latestEntryId])
        Metrics.synchronizationTimer(peerId, timeElapsed)
        isSynchronized = true
        clearMap()
    } else {
    }

    private fun clearMap() = entryIdToTime.keys().toList().forEach { entryIdToTime.remove(it) }

    private suspend fun getLatestEntryIdFromOtherPeers(currentEntryId: String): String? = protocolClient
        .sendLatestEntryIdQuery(otherConsensusPeers(), currentEntryId)
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

}