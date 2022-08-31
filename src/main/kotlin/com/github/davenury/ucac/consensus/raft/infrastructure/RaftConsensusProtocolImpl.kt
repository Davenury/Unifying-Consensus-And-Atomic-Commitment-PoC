package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.consensus.raft.domain.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import org.slf4j.LoggerFactory
import java.time.Duration

/** @author Kamil Jarosz */
class RaftConsensusProtocolImpl(
    private val peerId: Int,
    private val peersetId: Int,
    private var peerAddress: String,
    private val ctx: ExecutorCoroutineDispatcher,
    private var consensusPeers: List<String>,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val protocolClient: RaftProtocolClient,
    private val heartbeatDue: Duration = Duration.ofSeconds(4),
    private val leaderTimeout: Duration = Duration.ofMillis(500)
) : ConsensusProtocol<Change, History>, RaftConsensusProtocol, SignalSubject {

    private var currentTerm: Int = 0
    private val peerUrlToNextIndex: MutableMap<String, PeerIndexes> = mutableMapOf()
    private val ledgerIndexToMatchIndex: MutableMap<Int, Int> = mutableMapOf()
    private var votedFor: VotedFor? = null
    private var state: Ledger = Ledger()
    private var role: RaftRole = RaftRole.Candidate
    private var timer = ProtocolTimerImpl(Duration.ofSeconds(0), Duration.ofSeconds(2), ctx)

    override suspend fun begin() {
        logger.info("$peerId - Start raft on address $peerAddress, other peers: $consensusPeers")
        timer.startCounting { sendLeaderRequest() }
    }

    override fun getPeerName() = "peerset${peersetId}/peer${peerId}"

    override fun setOtherPeers(otherPeers: List<String>) {
        logger.info("$peerId - Peers: $otherPeers \n old: $consensusPeers")
        consensusPeers = otherPeers
    }

    private suspend fun sendLeaderRequest() {
        logger.info("Peer $peerId try to become leader in term $currentTerm")
        currentTerm += 1
        role = RaftRole.Candidate

        val responses = protocolClient.sendConsensusElectMe(
            consensusPeers,
            ConsensusElectMe(peerId, currentTerm, state.lastApplied)
        )

        logger.info("Responses from leader request for $peerId: $responses in iteration $currentTerm")

        currentTerm = responses.filterNotNull().maxOfOrNull { it.myIteration } ?: currentTerm

        val positiveResponses = responses.filterNotNull().filter { it.voteGranted }

        if (!checkHalfOfPeerSet(positiveResponses.size) || consensusPeers.isEmpty()) {
//            leader = null
//            leaderAddress = null
            role = RaftRole.Follower
            votedFor = null
            restartLeaderTimeout()
            return
        }

        logger.info("$peerId - I'm the leader in iteration $currentTerm")

        val leaderAffirmationReactions =
            protocolClient.sendConsensusImTheLeader(
                consensusPeers,
                ConsensusImTheLeader(peerId, peerAddress, currentTerm)
            )


        logger.info("Affirmations responses: $leaderAffirmationReactions")

//        leaderAddress = peerAddress
        votedFor = VotedFor(peerId,peerAddress)
        role = RaftRole.Leader

        // TODO - schedule heartbeat sending by leader
        timer = getLeaderTimer()
        timer.startCounting { sendHeartbeat() }
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastAcceptedId: Int): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (amILeader() || iteration <= currentTerm || lastAcceptedId < state.lastApplied) {
            return ConsensusElectedYou(this.peerId, currentTerm, false)
        }

        currentTerm = iteration
        restartLeaderTimeout()
        return ConsensusElectedYou(this.peerId, currentTerm, true)
    }

    override suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int) {
        if (this.currentTerm > term) return
        logger.info("${this.peerId} - Leader Elected! Is $peerId")
        votedFor = VotedFor(peerId, peerAddress)
        role = RaftRole.Follower
        restartLeaderTimeout()
    }

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): Boolean {
        val iteration = heartbeat.iteration
        val peerId = heartbeat.peerId
        val acceptedChanges = heartbeat.acceptedChanges.map { it.toLedgerItem() }
        val proposedChanges = heartbeat.proposedChanges.map { it.toLedgerItem() }

        if (iteration < currentTerm) {
            logger.info("HandleHeartbeat in ${this.peerId} from $peerId $iteration $currentTerm - true leader is ${votedFor?.id}")
            return false
        }
        logger.info("${this.peerId} - Received heartbeat from $peerId with \n newAcceptedChanges: $acceptedChanges \n newProposedChanges $proposedChanges")
        state.updateLedger(acceptedChanges, proposedChanges)

        acceptedChanges.lastOrNull()?.let {
            signalPublisher.signal(
                Signal.ConsensusAfterHandlingHeartbeat,
                this,
                listOf(this.consensusPeers),
                null,
                it.change
            )
        }

        restartLeaderTimeout()
        return true
    }

    override suspend fun handleProposeChange(change: Change) =
        proposeChange(change)

    override fun setPeerAddress(address: String) {
        this.peerAddress = address
    }

    override fun getLeaderAddress(): String? = votedFor?.address
    override fun getProposedChanges(): List<Change> = state.getProposedChanges()
    override fun getAcceptedChanges(): List<Change> = state.getAcceptedChanges()


    private suspend fun sendHeartbeat() {
        logger.info("SendHeartbeat from $peerId in $currentTerm to peers: $consensusPeers")
        val iteration = currentTerm
        val peersWithMessage = consensusPeers.map { peerUrl ->
            val peerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes(-1, -1))
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

                val peerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                val newAcceptedChanges = state.getNewAcceptedItems(peerIndexes.acceptedIndex)
                val newProposedChanges = state.getNewProposedItems(peerIndexes.acknowledgedIndex)


                if (newProposedChanges.isNotEmpty()) {
                    newProposedChanges.forEach {
                        ledgerIndexToMatchIndex[it.ledgerIndex] =
                            ledgerIndexToMatchIndex.getOrDefault(it.ledgerIndex, 0) + 1
                    }
                    peerUrlToNextIndex[peerUrl] =
                        peerIndexes.copy(acknowledgedIndex = newProposedChanges.maxOf { it.ledgerIndex })
                }
                if (newAcceptedChanges.isNotEmpty()) {
                    val newPeerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes(-1, -1))
                    peerUrlToNextIndex[peerUrl] =
                        newPeerIndexes.copy(acceptedIndex = newAcceptedChanges.maxOf { it.ledgerIndex })
                }
                heartbeatResponse!!.accepted
            }

        val acceptedIndexes: List<Int> = ledgerIndexToMatchIndex
            .filter { (key, value) ->
                checkHalfOfPeerSet(value)
            }
            .map { it.key }

        logger.info("$peerId accept indexes: $acceptedIndexes")
        state.acceptItems(acceptedIndexes)
        acceptedIndexes.forEach { ledgerIndexToMatchIndex.remove(it) }


        if (responses.filterNotNull().all { it.accepted }) {
            timer = getLeaderTimer()
            timer.startCounting { sendHeartbeat() }
        } else timer.startCounting {
            logger.info("$peerId - some peer increase iteration try to become leader \n $responses")
            sendLeaderRequest()
        }
    }

    private suspend fun restartLeaderTimeout() {
        timer.cancelCounting()
        timer = if (votedFor == null) getLeaderTimer() else getHeartbeatTimer()
        timer.startCounting {
            logger.info("$peerId - leader ${votedFor?.address} doesn't send heartbeat, start try to become leader")
            sendLeaderRequest()
        }
    }

    private suspend fun proposeChangeToLedger(changeWithAcceptNum: Change): ConsensusResult {
        if (state.changeAlreadyProposed(changeWithAcceptNum)) return ConsensusFailure
        val id = state.proposeChange(changeWithAcceptNum, currentTerm)

        ledgerIndexToMatchIndex[id] = 0

        timer.cancelCounting()
//      Propose change
        sendHeartbeat()
        signalPublisher.signal(Signal.ConsensusAfterProposingChange, this, listOf(consensusPeers), null)

        if (state.lastApplied != id) return ConsensusResultUnknown

//      If change accepted, propagate it
        timer.cancelCounting()
        sendHeartbeat()

        return ConsensusSuccess
    }

    private suspend fun sendRequestToLeader(change: Change): ConsensusResult = try {
        val response = httpClient.post<String>("http://${votedFor!!.address}/consensus/request_apply_change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }
        logger.info("Response from leader: $response")
        ConsensusSuccess
    } catch (e: Exception) {
        logger.info("$peerId - $e")
        ConsensusFailure
    }

    private suspend fun tryToProposeChangeMyself(changeWithAcceptNum: Change): ConsensusResult {
        val id = state.proposeChange(changeWithAcceptNum, currentTerm)
        ledgerIndexToMatchIndex[id] = 0
        timer.startCounting {
            logger.info("$peerId - change was proposed a no leader is elected")
            sendLeaderRequest()
        }
        return ConsensusSuccess
    }

    override suspend fun proposeChange(change: Change): ConsensusResult {
        // TODO
        logger.info("$peerId received change: $change")
        return when {
            amILeader() -> proposeChangeToLedger(change)
            votedFor != null -> sendRequestToLeader(change)
            else -> tryToProposeChangeMyself(change)
        }
    }

    override fun getState(): History {
        val history = state.getHistory()
        logger.info("$peerId - request for state: $history")
        return history
    }

    private fun checkHalfOfPeerSet(value: Int): Boolean = (value + 1) * 2 > (consensusPeers.size + 1)

    private fun amILeader(): Boolean = role == RaftRole.Leader

    private fun getLeaderTimer() = ProtocolTimerImpl(leaderTimeout, Duration.ZERO, ctx)
    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatDue, Duration.ofSeconds(2), ctx)

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
    }
}

data class PeerIndexes(val acceptedIndex: Int, val acknowledgedIndex: Int)

data class VotedFor(val id: Int, val address: String)