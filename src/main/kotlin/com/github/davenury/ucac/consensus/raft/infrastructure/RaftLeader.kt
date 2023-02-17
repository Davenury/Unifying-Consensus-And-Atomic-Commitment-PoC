package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
class RaftLeader(
    private var peerResolver: PeerResolver,
) {
    private val leaderRequestExecutorService =
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val propagationRequests: ConcurrentLinkedDeque<PropagationRequest> = ConcurrentLinkedDeque()

    private val mutex: Mutex = Mutex()
    private var id: GlobalPeerId? = null
    private var elected: Boolean = false
    private var currentTask: Job? = null

    suspend fun reset() = mutex.withLock {
        this.id = null
        this.elected = false
    }

    suspend fun voteFor(id: GlobalPeerId) = mutex.withLock {
        this.id = id
        this.elected = false
    }

    suspend fun elect(id: GlobalPeerId): List<PropagationRequest> = mutex.withLock {
        this.id = id
        this.elected = true

        logger.info("${id.peerId}  ${peerResolver.currentPeer().peerId}")

        if (currentTask?.isActive == true)
            currentTask?.cancel()

        return@withLock if (id == peerResolver.currentPeer()) {
            propagationRequests.toList()
        } else {
            currentTask = propagateChanges(id)
            listOf()
        }
    }

    suspend fun votedFor(): GlobalPeerId? = mutex.withLock {
        return this.id
    }

    suspend fun elected(): GlobalPeerId? = mutex.withLock {
        return if (elected) {
            this.id
        } else {
            null
        }
    }

    suspend fun isElected(): Boolean {
        return elected() != null
    }

    suspend fun propagateChange(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()
        propagationRequests.add(PropagationRequest(change, cf))

        val leaderId = elected()
        if (leaderId != null && currentTask?.isCompleted == false) {
            currentTask = propagateChanges(leaderId)
        } else {
            logger.info(
                "Change cannot be propagated to the leader, " +
                        "as it is not elected yet, queueing: $change"
            )
        }
        return cf
    }

    private fun propagateChanges(leaderId: GlobalPeerId): Job =
        with(CoroutineScope(leaderRequestExecutorService)) {
            launch {
                while (propagationRequests.isNotEmpty()) {
                    if (leaderId == peerResolver.currentPeer()) break
                    val req = propagationRequests.pop()
                    logger.info("Size: ${propagationRequests.size}")
                    req.cf.complete(sendChange(leaderId, req.change))
                }
            }
        }


    private suspend fun sendChange(
        leaderId: GlobalPeerId,
        change: Change,
    ): ChangeResult {
        logger.info("Propagating change to leader ($leaderId): $change")
        val leaderAddress: PeerAddress = peerResolver.resolve(leaderId)
        return try {
            val url = "http://${leaderAddress.address}/consensus/request_apply_change"
            val response = httpClient.post<ChangeResult>(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = change
            }
            logger.info("Response from leader: $response")
            response
        } catch (e: Exception) {
            logger.info("Request to leader (${leaderAddress.address}) failed", e)
            null
        } ?: ChangeResult(ChangeResult.Status.TIMEOUT)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("raft-leader")
    }
}

data class PropagationRequest(
    val change: Change,
    val cf: CompletableFuture<ChangeResult>,
)
