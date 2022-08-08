package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.AdditionalActionConsensus
import com.github.davenury.ucac.ConsensusTestAddon
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeWithAcceptNum
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.consensus.raft.domain.*
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Semaphore

/**
 * @author Kamil Jarosz
 */
class RaftConsensusProtocolImpl(
    private val peerId: Int,
    private val peerAddress: String,
    private val timer: ProtocolTimer,
    private var consensusPeers: List<String>,
    private val addons: Map<ConsensusTestAddon, AdditionalActionConsensus> = emptyMap(),
    private val protocolClient: RaftProtocolClient,
) : ConsensusProtocol<Change, History>, RaftConsensusProtocol {

    private var leaderIteration: Int = 0
    private val semaphore: Semaphore = Semaphore(1)
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
        logger.info("Peer $peerId try to become leader in iteration $leaderIteration")
        leaderIteration += 1
        leader = peerId

        val responses = protocolClient.sendConsensusElectMe(
            consensusPeers,
            ConsensusElectMe(peerId, leaderIteration, state.getLastAcceptedItemId())
        )


        logger.info("Responses from leader request for $peerId: $responses in iteration $leaderIteration")

        leaderIteration = responses.filterNotNull().maxOf { it.myIteration }

        val positiveResponses = responses.filterNotNull().filter { it.voteGranted }

        if (!checkHalfOfPeerSet(positiveResponses.size)) {
            leader = null
            leaderAddress = null
            restartLeaderTimeout()
            return
        }

        logger.info("$peerId - I'm the leader in iteration $leaderIteration")

        val leaderAffirmationReactions =
            protocolClient.sendConsensusImTheLeader(
                consensusPeers,
                ConsensusImTheLeader(peerId, peerAddress, leaderIteration)
            )


        logger.info("Affirmations responses: $leaderAffirmationReactions")

        leaderAddress = peerAddress

        // TODO - schedule heartbeat sending by leader
        timer.setDelay(leaderTimeout)
        timer.startCounting { sendHeartbeat() }
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastAcceptedId: Int): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (amILeader() || iteration <= leaderIteration || lastAcceptedId < state.getLastAcceptedItemId()) {
            return ConsensusElectedYou(this.peerId, leaderIteration, false)
        }

        leaderIteration = iteration
        restartLeaderTimeout()
        return ConsensusElectedYou(this.peerId, leaderIteration, true)
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

        logger.info("HandleHeartbeat in ${this.peerId} from $peerId $iteration $leaderIteration - true leader is $leader")
        if (iteration < leaderIteration || peerId != leader) return false
        logger.info("${this.peerId} - Received heartbeat from $peerId with \n newAcceptedChanges: $acceptedChanges \n newProposedChanges $proposedChanges")
        state.updateLedger(acceptedChanges, proposedChanges)

        restartLeaderTimeout()
        return true
    }

    override suspend fun handleProposeChange(change: ChangeWithAcceptNum) {
        proposeChange(change.change, change.acceptNum)
    }

    override fun getLeaderAddress(): String? = leaderAddress
    override fun getProposedChanges(): History = state.getProposedChanges()
    override fun getAcceptedChanges(): History = state.getAcceptedChanges()


    private suspend fun sendHeartbeat() {
        logger.info("SendHeartbeat from $peerId in $leaderIteration to peers: $consensusPeers")
        val iteration = leaderIteration
        val start = System.nanoTime()
        val peersWithMessage = consensusPeers.map { peerUrl ->
            val peerIndexes = peerUrlToLastPeerIndexes.getOrDefault(peerUrl, PeerIndexes(-1, -1))
            val newAcceptedChanges = state.getNewAcceptedItems(peerIndexes.acceptedIndex)
            val newProposedChanges = state.getNewProposedItems(peerIndexes.acknowledgedIndex)

            val message = ConsensusHeartbeat(
                peerId,
                iteration,
                newAcceptedChanges.map { it.toDto() },
                newProposedChanges.map { it.toDto() })

            Pair(peerUrl, message)
        }
        val responses = protocolClient.sendConsensusHeartbeat(peersWithMessage)
        val result: List<Boolean> = consensusPeers.zip(responses)
            .filter { it.second?.accepted ?: false }
            .map { pair ->

                val peerUrl = pair.first
                val peerIndexes = peerUrlToLastPeerIndexes.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                val newAcceptedChanges = state.getNewAcceptedItems(peerIndexes.acceptedIndex)
                val newProposedChanges = state.getNewProposedItems(peerIndexes.acknowledgedIndex)


                if (newProposedChanges.isNotEmpty()) {
                    newProposedChanges.forEach {
                        ledgerIdToVoteGranted[it.id] =
                            ledgerIdToVoteGranted.getOrDefault(it.id, 0) + 1
                    }
                    peerUrlToLastPeerIndexes[peerUrl] =
                        peerIndexes.copy(acknowledgedIndex = newProposedChanges.maxOf { it.id })
                }
                if (newAcceptedChanges.isNotEmpty()) {
                    val newPeerIndexes = peerUrlToLastPeerIndexes.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                    peerUrlToLastPeerIndexes[peerUrl] =
                        newPeerIndexes.copy(acceptedIndex = newAcceptedChanges.maxOf { it.id })
                }
                pair.second!!.accepted
            }

        val time = Duration.ofNanos(System.nanoTime() - start)
        val acceptedIndexes: List<Int> = ledgerIdToVoteGranted
            .filter { (key, value) ->
                checkHalfOfPeerSet(value)
            }
            .map { it.key }

        logger.info("$peerId accept indexes: $acceptedIndexes")
        state.acceptItems(acceptedIndexes)
        acceptedIndexes.forEach { ledgerIdToVoteGranted.remove(it) }

        if (responses.all { it?.accepted == true }) timer.startCounting { sendHeartbeat() }
        else timer.startCounting {
            logger.info("$peerId - some peer increase iteration try to become leader")
            sendLeaderRequest()
        }
    }

    private suspend fun restartLeaderTimeout() {
        timer.cancelCounting()
        timer.setDelay(if (leader == null) leaderTimeout else heartbeatDue)
        timer.startCounting {
            logger.info("$peerId - leader $leaderAddress doesn't send heartbeat, start try to become leader")
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
        } else if(leaderAddress != null) {
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
        } else{
            val id = state.proposeChange(changeWithAcceptNum, leaderIteration)
            ledgerIdToVoteGranted[id] = 0
            timer.startCounting {
                logger.info("$peerId - change was proposed a no leader is elected")
                sendLeaderRequest()
            }
            return ConsensusSuccess
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
        private val heartbeatDue = Duration.ofSeconds(6)
        private val leaderTimeout = Duration.ofMillis(500)
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
