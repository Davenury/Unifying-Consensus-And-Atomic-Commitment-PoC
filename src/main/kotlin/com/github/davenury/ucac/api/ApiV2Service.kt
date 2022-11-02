package com.github.davenury.ucac.api

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.gpac.domain.GPACProtocol
import com.github.davenury.ucac.history.History
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
    private val history: History,
    private val config: Config,
) {

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)
            ?.let { Change.fromHistoryEntry(it) }
    }

    suspend fun addChange(change: Change): CompletableFuture<ChangeResult> {
        val peers = change.peers
        return if (allPeersFromMyPeerset(peers)) {
            consensusProtocol.proposeChangeAsync(change)
        } else {
            gpacProtocol.proposeChangeAsync(change)
        }
    }

    suspend fun addChangeSync(
        change: Change,
        timeout: Duration?,
    ): ChangeResult? {
        return try {
            withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
                addChange(change).await()
            }
        } catch (e: TimeoutCancellationException) {
            null
        }
    }

    private fun allPeersFromMyPeerset(peers: List<String>) =
        config.peerAddresses(config.peersetId).containsAll(peers)
}
