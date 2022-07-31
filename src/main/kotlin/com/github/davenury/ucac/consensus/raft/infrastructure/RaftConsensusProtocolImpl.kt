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
    private var consensusPeers: List<String>,
    private val addons: Map<ConsensusTestAddon, AdditionalActionConsensus> = emptyMap()
) :
    ConsensusProtocol<Change, History>,
    RaftConsensusProtocol {

    private var leaderIteration: Int = 0
    private val voteGranted: Map<PeerId, ConsensusPeer> = mutableMapOf()
    private val peerUrlToLastPeerIndexes: MutableMap<String, PeerIndexes> = mutableMapOf()
    private val ledgerIdToVoteGranted: MutableMap<Int, Int> = mutableMapOf()
    private var leader: Int? = null
    private var leaderAddress: String? = null
    private var state: Ledger = Ledger()

    override suspend fun begin() {
        logger.info("$peerId - Start raft on address $peerAddress, other peers: $consensusPeers")
        timer.startCounting { sendLeaderRequest() }
    }

    private suspend fun sendLeaderRequest() {
        leaderIteration += 1
        leader = peerId
        val responses =
            consensusPeers
                .mapNotNull { peerUrl ->
                    try {
                        httpClient.post<ConsensusElectedYou>(
                            "http://$peerUrl/consensus/request_vote"
                        ) {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body = ConsensusElectMe(peerId, leaderIteration, state.getLastAcceptedItemId())
                        }
                    } catch (e: Exception) {
                        logger.info("$peerId - error $e when sending request to $peerUrl")
                        null
                    }
                }

        logger.info("Responses from leader request for $peerId: $responses in iteration $leaderIteration")

        val positiveResponses = responses.filter { it.voteGranted }

        if (!checkHalfOfPeerSet(positiveResponses.size)) {
            leader = null
            leaderAddress = null
            restartLeaderTimeout()
            return
        }

        logger.info("$peerId - I'm the leader in iteration $leaderIteration")

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

        leaderAddress = peerAddress

        // TODO - schedule heartbeat sending by leader
        val halfDelay: Duration = heartbeatDue.dividedBy(4)
        timer.setDelay(halfDelay)
        timer.startCounting { sendHeartbeat() }
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastAcceptedId: Int): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (amILeader() || iteration <= leaderIteration || lastAcceptedId < state.getLastAcceptedItemId()) {
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
        iteration: Int,
        acceptedChanges: List<LedgerItem>,
        proposedChanges: List<LedgerItem>
    ): Boolean {

        if (iteration < leaderIteration) return false
        logger.info("${this.peerId} - Received heartbeat with \n newAcceptedChanges: $acceptedChanges \n newProposedChanges $proposedChanges")
        state.updateLedger(acceptedChanges, proposedChanges)

        restartLeaderTimeout()
        return true
    }

    override suspend fun handleProposeChange(change: ChangeWithAcceptNum) {
        if (amILeader()) proposeChange(change.change, change.acceptNum)
    }

    override fun getLeaderAddress(): String? = leaderAddress
    override fun getProposedChanges(): History = state.getProposedChanges()
    override fun getAcceptedChanges(): History = state.getAcceptedChanges()


    private suspend fun sendHeartbeat() {
        consensusPeers.map { peerUrl ->
            try {
                val peerIndexes = peerUrlToLastPeerIndexes.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                val newAcceptedChanges = state.getNewAcceptedItems(peerIndexes.acceptedIndex)
                val newProposedChanges = state.getNewProposedItems(peerIndexes.acknowledgedIndex)

                val response = httpClient.post<String>("http://$peerUrl/consensus/heartbeat") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = ConsensusHeartbeat(
                        peerId,
                        leaderIteration,
                        newAcceptedChanges.map { it.toDto() },
                        newProposedChanges.map { it.toDto() })
                }

                if (newProposedChanges.isNotEmpty()) {
                    newProposedChanges.forEach {
                        ledgerIdToVoteGranted[it.id] =
                            ledgerIdToVoteGranted.getOrDefault(it.id, 0) + 1
                    }
                    peerUrlToLastPeerIndexes[peerUrl] =
                        peerIndexes.copy(acknowledgedIndex = newProposedChanges.last().id)
                }
                if (newAcceptedChanges.isNotEmpty()) {
                    val previousAcceptedIndex = peerUrlToLastPeerIndexes.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                    val newAcceptedIndex: Int =
                        newAcceptedChanges.lastOrNull()?.id ?: previousAcceptedIndex.acceptedIndex
                    peerUrlToLastPeerIndexes[peerUrl] = peerIndexes.copy(acceptedIndex = newAcceptedIndex)
                }

            } catch (e: Exception) {
                logger.warn("$peerId - $e")
            }
        }
        val acceptedIndexes: List<Int> = ledgerIdToVoteGranted
            .filter { (key, value) ->
                checkHalfOfPeerSet(value)
            }
            .map { it.key }

        state.acceptItems(acceptedIndexes)

        timer.startCounting { sendHeartbeat() }
    }

    private suspend fun restartLeaderTimeout() {
        timer.cancelCounting()
        timer.setDelay(heartbeatDue)
        timer.startCounting {
            logger.info("$peerId - leader not send heartbeat, start try to become leader")
            sendLeaderRequest()
        }
    }


    override suspend fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
        val changeWithAcceptNum = ChangeWithAcceptNum(change, acceptNum)
        logger.info("$peerId received change: $changeWithAcceptNum")
        if (amILeader()) {

            if (state.changeAlreadyProposed(changeWithAcceptNum)) return ConsensusFailure
            val id = state.proposeChange(changeWithAcceptNum, leaderIteration)

            ledgerIdToVoteGranted[id] = 0

            timer.cancelCounting()
            sendHeartbeat()
            addons[ConsensusTestAddon.AfterProposingChange]?.invoke()
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
        val history = state.getHistory()
        logger.info("$peerId - request for state: $history")
        return history
    }

    private fun checkHalfOfPeerSet(value: Int): Boolean = (value + 1) * 2 > (consensusPeers.size + 1)

    private fun amILeader(): Boolean = leader == peerId

    override fun setOtherPeers(otherPeers: List<String>) {
        this.consensusPeers = otherPeers
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
        private val heartbeatDue = Duration.ofSeconds(4)
    }
}

data class PeerIndexes(val acceptedIndex: Int, val acknowledgedIndex: Int)

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
