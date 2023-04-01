package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.request.*
import io.ktor.client.response.*
import io.ktor.client.statement.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors

enum class AffinityWaitingResult {
    LEADER_ALIVE,
    TIMEOUT,
    NO_AFFINITY
}

class AffinityHandler(
    private val consensusAffinity: Map<PeersetId, PeerId>,
    private val peerResolver: PeerResolver,
    private val peersetId: PeersetId,
    private val leaderAliveTimeout: Duration,
) {

    private var shouldTryToCheckLeader = true
    private var waitingForAffinityResult: AffinityWaitingResult? = null

    fun amIAffinityLeader(): Boolean =
        consensusAffinity.isEmpty() || consensusAffinity[peersetId] == null || consensusAffinity[peersetId] == peerResolver.currentPeer()

    fun amITrueAffinityLeader(): Boolean {
        if (consensusAffinity.isEmpty() || consensusAffinity[peersetId] == null) {
            return false
        }
        return consensusAffinity[peersetId] == peerResolver.currentPeer()
    }

    suspend fun waitForAffinityLeaderToBeAlive(): AffinityWaitingResult {
        ctx.dispatch(Dispatchers.IO) {
            timeout()
        }

        if (consensusAffinity.isEmpty()) {
            return AffinityWaitingResult.NO_AFFINITY.also {
                waitingForAffinityResult = it
            }
        }

        if (consensusAffinity[peersetId] == null) {
            return AffinityWaitingResult.NO_AFFINITY.also {
                waitingForAffinityResult = it
            }
        }

        var isAlive = false
        while (!isAlive && shouldTryToCheckLeader) {
            delay(leaderAliveInterval.toMillis())
            isAlive = checkAffinityLeader(consensusAffinity[peersetId]!!)
            logger.warn("Leader is alive: $isAlive")
        }

        if (!shouldTryToCheckLeader) {
            logger.warn("Leader is not alive during the timeout")
            return AffinityWaitingResult.TIMEOUT.also {
                waitingForAffinityResult = it
            }
        }

        return AffinityWaitingResult.LEADER_ALIVE.also {
            waitingForAffinityResult = it
        }
    }

    private suspend fun checkAffinityLeader(peerId: PeerId): Boolean {
        return try {
            raftHttpClient.get<HttpStatement>("http://${peerResolver.resolve(peerId).address}/_meta/health").execute()
            true
        } catch (e: Exception) {
            logger.warn("Couldn't get $peerId", e)
            false
        }
    }

    private fun timeout() {
        runBlocking {
            delay(leaderAliveTimeout.toMillis())
            shouldTryToCheckLeader = false
        }
    }

    fun shouldRestartAffinityTimer(peerId: PeerId): Boolean {
        if (waitingForAffinityResult == null && consensusAffinity[peersetId] == peerId) {
            return true
        }
        if (waitingForAffinityResult != AffinityWaitingResult.LEADER_ALIVE) {
            logger.debug(
                "Waiting for affinity result is not LEADER ALIVE, so I won't restart affinity timer, result is: {}",
                waitingForAffinityResult
            )
            return false
        }
        logger.debug("Consensus affinity: {}, peer in ask: {}", consensusAffinity[peersetId], peerId)
        return consensusAffinity[peersetId] == peerId
    }

    companion object {
        private val leaderAliveInterval = Duration.ofMillis(500)

        private val logger = LoggerFactory.getLogger("ConsensusAffinityHandler")
        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}