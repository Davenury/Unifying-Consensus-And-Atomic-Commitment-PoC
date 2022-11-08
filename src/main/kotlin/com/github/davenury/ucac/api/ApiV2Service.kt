package com.github.davenury.ucac.api

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.common.Changes
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.commitment.gpac.GPACProtocol
import com.github.davenury.ucac.common.ChangeDoesntExist
import com.github.davenury.ucac.history.History
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque

class ApiV2Service(
    private val gpacProtocol: GPACProtocol,
    private val consensusProtocol: ConsensusProtocol,
    private val history: History,
    private var config: Config,
) {

    private val channel: Channel<Unit> = Channel()
    private val queue: Deque<ProcessorJob> = ConcurrentLinkedDeque<ProcessorJob>()
    private val worker: Thread = Thread(Worker(queue, channel, gpacProtocol, consensusProtocol))

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
        ?: throw ChangeDoesntExist()

    suspend fun addChange(change: Change, enforceGpac: Boolean = false): CompletableFuture<ChangeResult> =
        CompletableFuture<ChangeResult>().also {
            val isConsensusChange: Boolean = allPeersFromMyPeerset(change.peers) && !enforceGpac
            queue.addLast(ProcessorJob(change, it, isConsensusChange))
            channel.send(Unit)
        }

    suspend fun addChangeSync(
        change: Change,
        enforceGpac: Boolean,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addChange(change, enforceGpac).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    fun setPeers(peers: Map<Int, List<String>>, myAddress: String) {
        val myPeerset = peers[config.peersetId]!!.plus(myAddress).distinct()
        val newPeersString = peers
            .plus(config.peersetId to myPeerset)
            .map { it.value.joinToString(",") }
            .joinToString(";")
        config = config.copy(peers = newPeersString)
    }

    private fun allPeersFromMyPeerset(peers: List<String>) =
        config.peerAddresses(config.peersetId).containsAll(peers)
}
