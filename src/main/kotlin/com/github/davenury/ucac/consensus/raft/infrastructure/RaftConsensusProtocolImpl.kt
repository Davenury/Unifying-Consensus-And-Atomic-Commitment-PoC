package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.consensus.raft.domain.*
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * @author Kamil Jarosz
 */
class RaftConsensusProtocolImpl(
    private val peerId: Int,
    private val peersetId: Int,
    private val timer: ProtocolTimer,
    private var consensusPeers: List<String>
) :
    ConsensusProtocol<Change, History>,
    RaftConsensusProtocol {

    private val voteGranted: Map<PeerId, ConsensusPeer> = mutableMapOf()
    private var amLeader = false
    private var isLeader = false

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
        return ConsensusSuccess
    }

    override fun getState(): History? {
        // TODO
        return emptyList<ChangeWithAcceptNum>() as History
    }

    override suspend fun begin() {
        logger.info("Scheduling task")
        timer.startCounting { sendLeaderRequest() }
    }

    private suspend fun sendLeaderRequest() {
        if (isLeader) {
            return
        }
        val responses = consensusPeers.mapNotNull { peerUrl ->
            try {
                httpClient.post<ConsensusElectedYou>("http://$peerUrl/consensus/request_vote") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = ConsensusElectMe(peerId)
                }
            } catch (e: Exception) {
                null
            }
        }.filter { it.voteGranted }

        logger.info("Responses from leader request for $peerId: $responses")

        if (responses.size + 1 < (consensusPeers.size + 1).toFloat() / 2F) {
            timer.startCounting { sendLeaderRequest() }
            return
        }

        amLeader = true
        isLeader = true
        logger.info("$peerId - I'm the leader")

        val leaderAffirmationReactions = consensusPeers.map { peerUrl ->
            try {
                httpClient.post<String>("http://$peerUrl/consensus/leader") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = ConsensusImTheLeader(peerId)
                }.let {
                    "$it from $peerUrl"
                }
            } catch (e: Exception) {
                "$peerId - $e"
            }
        }

        logger.info("Affirmations responses: $leaderAffirmationReactions")

        // TODO - schedule heartbeat sending
    }

    override suspend fun handleRequestVote(message: ConsensusElectMe): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (isLeader) {
            return ConsensusElectedYou(peerId, false)
        }
        return ConsensusElectedYou(peerId, true)
    }

    override suspend fun handleLeaderElected(message: ConsensusImTheLeader) {
        logger.info("$peerId - Leader Elected! Is ${message.peerId}")
        isLeader = true
        timer.cancelCounting()
        timer.startCounting {
            // TODO: heartbeat request
        }
    }

    override fun setOtherPeers(otherPeers: List<String>) {
        this.consensusPeers = otherPeers
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
    }
}

data class PeerId(val id: Int)
data class ConsensusPeer(
    val peerId: PeerId,
    val voteGranted: Boolean,
    val rpcDue: Duration,
    val heartbeatDue: Duration,
    val matchIndex: Int,
    // maybe optional
    val nextIndex: Int
)
