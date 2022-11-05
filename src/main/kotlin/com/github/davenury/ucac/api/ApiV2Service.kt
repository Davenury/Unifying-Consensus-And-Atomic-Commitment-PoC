package com.github.davenury.ucac.api

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.commitment.gpac.GPACProtocol
import com.github.davenury.ucac.history.History
import io.ktor.features.*
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
    private val history: History,
    private var config: Config,
) {

    private val changeIdToCompletableFuture: Map<String, CompletableFuture<ChangeResult>> = mapOf()

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)
            ?.let { Change.fromHistoryEntry(it) }
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> = consensusProtocol.getChangeResult(changeId)
        ?: gpacProtocol.getChangeResult(changeId)
        ?: throw RuntimeException("Change doesn't exist")

    suspend fun addChange(change: Change, enforceGpac: Boolean = false): CompletableFuture<ChangeResult> =
        if (allPeersFromMyPeerset(change.peers) && !enforceGpac) {
            consensusProtocol.proposeChangeAsync(change)
        } else {
            gpacProtocol.proposeChangeAsync(change)
        }


    suspend fun addChangeSync(
        change: Change,
        enforceGpac: Boolean,
        timeout: Duration?,
    ): ChangeResult? {
        return try {
            withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
                addChange(change, enforceGpac).await()
            }
        } catch (e: TimeoutCancellationException) {
            null
        }
    }

    fun setPeers(peers: Map<Int, List<String>>, myAddress: String) {

        val myPeerset = peers[config.peersetId]!!.plus(myAddress)
        val newPeersString = peers
            .plus(config.peersetId to myPeerset)
            .map { it.value.joinToString(",") }
            .joinToString(";")
        config = config.copy(peers = newPeersString)
    }

    private fun allPeersFromMyPeerset(peers: List<String>) =
        config.peerAddresses(config.peersetId).containsAll(peers)
}
