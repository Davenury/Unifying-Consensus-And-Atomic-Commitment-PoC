package com.example.consensus.raft.infrastructure

import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.raft.domain.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import java.time.Duration
import org.slf4j.LoggerFactory
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import java.util.concurrent.Semaphore

/** @author Kamil Jarosz */
class RaftConsensusProtocolImpl(
    private val peerId: Int,
    var peerAddress: String,
    private val ctx: ExecutorCoroutineDispatcher,
    private var consensusPeers: List<String>,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val protocolClient: RaftProtocolClient,
    private val heartbeatDue: Duration = Duration.ofSeconds(4),
    private val leaderTimeout: Duration = Duration.ofMillis(500)
) : ConsensusProtocol<Change, History>, RaftConsensusProtocol, SignalSubject {

    private var leaderIteration: Int = 0
    private val peerUrlToLastPeerIndexes: MutableMap<String, PeerIndexes> = mutableMapOf()
    private val ledgerIdToVoteGranted: MutableMap<Int, Int> = mutableMapOf()
    private var leader: Int? = null
    private var leaderAddress: String? = null
    private var state: Ledger = Ledger()
    private var timer = ProtocolTimerImpl(Duration.ofSeconds(0), Duration.ofSeconds(2), ctx)

    override suspend fun begin() {
        logger.info("$peerId - Start raft on address $peerAddress, other peers: $consensusPeers")
        timer.startCounting { sendLeaderRequest() }
    }

    override fun setOtherPeers(otherPeers: List<String>) {
        println("$peerId - Peers: $otherPeers \n old: $consensusPeers")
        consensusPeers = otherPeers
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

        leaderIteration = responses.filterNotNull().maxOfOrNull { it.myIteration } ?: leaderIteration

        val positiveResponses = responses.filterNotNull().filter { it.voteGranted }

        if (!checkHalfOfPeerSet(positiveResponses.size) || consensusPeers.isEmpty()) {
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
        timer = getLeaderTimer()
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

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): Boolean {
        val iteration = heartbeat.iteration
        val peerId = heartbeat.peerId
        val acceptedChanges = heartbeat.acceptedChanges.map { it.toLedgerItem() }
        val proposedChanges = heartbeat.proposedChanges.map { it.toLedgerItem() }

        logger.info("HandleHeartbeat in ${this.peerId} from $peerId $iteration $leaderIteration - true leader is $leader")
        if (iteration < leaderIteration || peerId != leader) return false
        logger.info("${this.peerId} - Received heartbeat from $peerId with \n newAcceptedChanges: $acceptedChanges \n newProposedChanges $proposedChanges")
        state.updateLedger(acceptedChanges, proposedChanges)

        restartLeaderTimeout()
        return true
    }

    override suspend fun handleProposeChange(change: ChangeWithAcceptNum) = proposeChange(change.change, change.acceptNum)

    override fun setLeaderAddress(address: String) {
        peerAddress = address
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
            .map { (peerUrl, heartbeatResponse) ->

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
                heartbeatResponse!!.accepted
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


        if (responses.all { it?.accepted == true }) {
            timer = getLeaderTimer()
            timer.startCounting { sendHeartbeat() }
        } else timer.startCounting {
            logger.info("$peerId - some peer increase iteration try to become leader")
            sendLeaderRequest()
        }
    }

    private suspend fun restartLeaderTimeout() {
        timer.cancelCounting()
        timer = if (leader == null) getLeaderTimer() else getHeartbeatTimer()
        timer.startCounting {
            logger.info("$peerId - leader $leaderAddress doesn't send heartbeat, start try to become leader")
            sendLeaderRequest()
        }
    }

    private suspend fun proposeChangeToLedger(changeWithAcceptNum: ChangeWithAcceptNum): ConsensusResult {
        if (state.changeAlreadyProposed(changeWithAcceptNum)) return ConsensusFailure
        val id = state.proposeChange(changeWithAcceptNum, leaderIteration)

        ledgerIdToVoteGranted[id] = 0

        timer.cancelCounting()
//      Propose change
        sendHeartbeat()
        signalPublisher.signal(Signal.ConsensusAfterProposingChange, this, listOf(consensusPeers), null)

        if (state.getLastAcceptedItemId() != id) return ConsensusResultUnknown

//      If change accepted, propagate it
        timer.cancelCounting()
        sendHeartbeat()

        return ConsensusSuccess
    }

    private suspend fun sendRequestToLeader(changeWithAcceptNum: ChangeWithAcceptNum): ConsensusResult = try {
        val response = httpClient.post<String>("http://$leaderAddress/consensus/request_apply_change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = ConsensusProposeChange(changeWithAcceptNum.toDto())
            }
        println("Response from leader: $response")
        ConsensusSuccess
    } catch (e: Exception) {
        logger.info("$peerId - $e")
        ConsensusFailure
    }

    private suspend fun tryToProposeChangeMyself(changeWithAcceptNum: ChangeWithAcceptNum): ConsensusResult {
        val id = state.proposeChange(changeWithAcceptNum, leaderIteration)
        ledgerIdToVoteGranted[id] = 0
        timer.startCounting {
            logger.info("$peerId - change was proposed a no leader is elected")
            sendLeaderRequest()
        }
        return ConsensusSuccess
    }

    override suspend fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
        val changeWithAcceptNum = ChangeWithAcceptNum(change, acceptNum)
        logger.info("$peerId received change: $changeWithAcceptNum")
        return when {
            amILeader() -> proposeChangeToLedger(changeWithAcceptNum)
            leaderAddress != null -> sendRequestToLeader(changeWithAcceptNum)
            else -> tryToProposeChangeMyself(changeWithAcceptNum)
        }
    }

    override fun getState(): History {
        val history = state.getHistory()
        logger.info("$peerId - request for state: $history")
        return history
    }

    private fun checkHalfOfPeerSet(value: Int): Boolean = (value + 1) * 2 > (consensusPeers.size + 1)

    private fun amILeader(): Boolean = leader == peerId

    private fun getLeaderTimer() = ProtocolTimerImpl(leaderTimeout, Duration.ZERO, ctx)
    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatDue, Duration.ofSeconds(2), ctx)

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
    }
}

data class PeerIndexes(val acceptedIndex: Int, val acknowledgedIndex: Int)

data class PeerId(val id: Int)

data class ConsensusPeer(
    val peerId: Int,
    val peerAddress: String
)
