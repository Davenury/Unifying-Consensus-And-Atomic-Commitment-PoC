package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeDoesntExist
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Changes
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.commitment.TwoPC.TwoPC
import com.github.davenury.ucac.commitment.gpac.GPACProtocol
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
    private val twoPC: TwoPC,
    private val history: History,
    private var config: Config,
    private var peerResolver: PeerResolver,
) {
    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)
    private val worker: Thread = Thread(Worker(queue, gpacProtocol, consensusProtocol, twoPC))

    init {
        worker.start()
    }

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> = consensusProtocol.getChangeResult(changeId)
        ?: gpacProtocol.getChangeResult(changeId)
        ?: throw ChangeDoesntExist(changeId)

    suspend fun addChange(change: Change, enforceGpac: Boolean, useTwoPC: Boolean): CompletableFuture<ChangeResult> =
        CompletableFuture<ChangeResult>().also {
            val isOnePeersetChange: Boolean = allPeersFromMyPeerset(change.peers)
            queue.send(ProcessorJob(change, it, isOnePeersetChange, enforceGpac, useTwoPC))
        }

    suspend fun addChangeSync(
        change: Change,
        enforceGpac: Boolean,
        useTwoPC: Boolean,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addChange(change, enforceGpac, useTwoPC).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    private fun allPeersFromMyPeerset(peers: List<String>) =
        peerResolver.getPeersFromCurrentPeerset().map { it.address }.containsAll(peers)
}
