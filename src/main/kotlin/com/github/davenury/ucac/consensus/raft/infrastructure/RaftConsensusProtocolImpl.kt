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
    private val peerAddress: String,
    private val timer: ProtocolTimer,
    private var consensusPeers: List<String>
) :
    ConsensusProtocol<Change, History>,
    RaftConsensusProtocol {

    private var leaderIteration: Int = 0
    private val voteGranted: Map<PeerId, ConsensusPeer> = mutableMapOf()
    private val peerUrlToLastAcceptedIndex: MutableMap<String, Int> = mutableMapOf()
    private val acceptedIndexToVoteGranted: MutableMap<Int, Int> = mutableMapOf()
    private var leader: Int? = null
    private var leaderAddress: String? = null
    private var state: History = mutableListOf()
    private var proposedChanges: History = mutableListOf()

    override suspend fun begin() {
        logger.info("Scheduling task")
        timer.startCounting { sendLeaderRequest() }
    }

    private suspend fun sendLeaderRequest() {
        leaderIteration += 1
        leader = peerId
        timer.setDelay(heartbeatDue.toMillis().toInt())
        val responses =
            consensusPeers
                .mapNotNull { peerUrl ->
                    try {
                        httpClient.post<ConsensusElectedYou>(
                            "http://$peerUrl/consensus/request_vote"
                        ) {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body = ConsensusElectMe(peerId, leaderIteration)
                        }
                    } catch (e: Exception) {
                        logger.info("$peerId - $e")
                        null
                    }
                }
                .filter { it.voteGranted }

        logger.info("Responses from leader request for $peerId: $responses in iteration $leaderIteration")

        if (!checkHalfOfPeerSet(responses.size)) {
            leader = null
            restartLeaderTimeout()
            return
        }

        logger.info("$peerId - I'm the leader")

        val leaderAffirmationReactions =
            consensusPeers.map { peerUrl ->
                try {
                    httpClient
                        .post<String>("http://$peerUrl/consensus/leader") {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body = ConsensusImTheLeader(peerId, peerAddress, leaderIteration)
                        }
                        .let { "$it from $peerUrl" }
                } catch (e: Exception) {
                    "$peerId - $e"
                }
            }

        logger.info("Affirmations responses: $leaderAffirmationReactions")

        // TODO - schedule heartbeat sending by leader
        val halfDelay: Int = heartbeatDue.toMillis().toInt() / 2
        timer.setDelay(halfDelay)
        timer.startCounting { sendHeartbeat() }
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (amILeader() || iteration <= leaderIteration) {
            return ConsensusElectedYou(this.peerId, false)
        }

        leaderIteration = iteration
        restartLeaderTimeout()
        return ConsensusElectedYou(this.peerId, true)
    }

    override suspend fun handleLeaderElected(peerId: Int, peerAddress: String, iteration: Int) {
        logger.info("${this.peerId} - Leader Elected! Is $peerId")
        leader = peerId
        leaderAddress = peerAddress
        restartLeaderTimeout()
    }

    override suspend fun handleHeartbeat(
        peerId: Int,
        acceptedChanges: List<ChangeWithAcceptNum>,
        proposedChanges: List<ChangeWithAcceptNum>
    ) {
        logger.info("${this.peerId} - Received heartbeat with \n newAcceptedChanges: $acceptedChanges \n newProposedChanges $proposedChanges")
        state.addAll(proposedChanges)
        restartLeaderTimeout()
    }

    override suspend fun handleProposeChange(change: ChangeWithAcceptNum) {
        proposeChange(change.change, change.acceptNum)
    }

    private suspend fun sendHeartbeat() {
        val changes = state + proposedChanges
        consensusPeers.map { peerUrl ->
            try {
                val acceptedIndex: Int = peerUrlToLastAcceptedIndex.getOrDefault(peerUrl, -1)
                val newPeerChanges = changes.filterIndexed { index, _ -> index > acceptedIndex }
                val newProposedChanges = proposedChanges.filterIndexed { index, _ -> index > acceptedIndex }

                val response = httpClient.post<String>("http://$peerUrl/consensus/heartbeat") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = ConsensusHeartbeat(
                        peerId,
                        newPeerChanges.map { it.toDto() },
                        newProposedChanges.map { it.toDto() })
                }

                if (response == "OK" && newProposedChanges.isNotEmpty()) {
                    val previousAcceptedIndex = peerUrlToLastAcceptedIndex.getOrDefault(peerUrl, 0)
                    val newAcceptedIndex = previousAcceptedIndex + newPeerChanges.size + newProposedChanges.size

                    peerUrlToLastAcceptedIndex[peerUrl] = newAcceptedIndex
                    newProposedChanges.forEach {
                        acceptedIndexToVoteGranted[newAcceptedIndex] =
                            acceptedIndexToVoteGranted.getOrDefault(it.acceptNum, 1) + 1
                    }
                }

            } catch (e: Exception) {
                logger.warn("$peerId - $e")
            }
        }
        val acceptedIndexes: List<Int> = acceptedIndexToVoteGranted
            .filter { (key, value) -> checkHalfOfPeerSet(value) }
            .map { it.key }

        val offset = state.size

        val acceptedChanges =
            proposedChanges.filterIndexed { index, _ -> acceptedIndexes.contains(index + offset) }

        proposedChanges.removeAll(acceptedChanges)
        state.addAll(acceptedChanges)
        timer.startCounting { sendHeartbeat() }
    }

    private suspend fun restartLeaderTimeout() {
        timer.cancelCounting()
        timer.setDelay(heartbeatDue.toMillis().toInt())
        timer.startCounting {
            sendLeaderRequest()
        }
    }


    override suspend fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
        val changeWithAcceptNum = ChangeWithAcceptNum(change, acceptNum)
        logger.info("$peerId received change: $changeWithAcceptNum")
        if (amILeader()) {

            proposedChanges.add(changeWithAcceptNum)
            acceptedIndexToVoteGranted[state.size + proposedChanges.size] = 1

            timer.cancelCounting()
            sendHeartbeat()
            return ConsensusSuccess
        } else {
            return try {
                httpClient
                    .post<String>("http://$leaderAddress/consensus/request_apply_change") {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = ConsensusProposeChange(changeWithAcceptNum.toDto())
                    }
                ConsensusSuccess
            } catch (e: Exception) {
                "$peerId - $e"
                ConsensusFailure
            }
        }


    }

    override fun getState(): History? {
        logger.info("$peerId - request for state: $state")
        return state
    }

    private fun checkHalfOfPeerSet(value: Int): Boolean = value + 1 > (consensusPeers.size + 1).toFloat() / 2F

    private fun amILeader(): Boolean = leader == peerId

    override fun setOtherPeers(otherPeers: List<String>) {
        this.consensusPeers = otherPeers
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
        private val heartbeatDue = Duration.ofSeconds(4)
    }
}


data class PeerId(val id: Int)

data class ConsensusPeer(
    val peerId: Int,
    val peerAddress: String
)

//data class ConsensusPeer(
//    val peerId: PeerId,
//    val voteGranted: Boolean,
//    val rpcDue: Duration,
//    val heartbeatDue: Duration,
//    val matchIndex: Int,
//    // maybe optional
//    val nextIndex: Int
//)
