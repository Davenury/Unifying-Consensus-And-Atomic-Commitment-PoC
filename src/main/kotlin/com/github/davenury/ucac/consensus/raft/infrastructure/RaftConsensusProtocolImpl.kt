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
//      Vote for myself
        votedFor = VotedFor(peerId, peerAddress)

        val responses = protocolClient.sendConsensusElectMe(
            consensusPeers,
            ConsensusElectMe(peerId, currentTerm, state.lastApplied)
        ).map { it.message }

        logger.info("Responses from leader request for $peerId: $responses in iteration $currentTerm")

        currentTerm = responses.filterNotNull().maxOfOrNull { it.myTerm } ?: currentTerm

        val positiveResponses = responses.filterNotNull().filter { it.voteGranted }

        if (!checkHalfOfPeerSet(positiveResponses.size) || consensusPeers.isEmpty()) {
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

        role = RaftRole.Leader

        peerUrlToNextIndex.keys.forEach {
            peerUrlToNextIndex.replace(it, PeerIndexes(state.lastApplied, state.lastApplied))
        }

        // TODO - schedule heartbeat sending by leader
        timer = getLeaderTimer()
        timer.startCounting { sendHeartbeat() }
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogIndex: Int): ConsensusElectedYou {
        // TODO - transaction blocker?
        if (amILeader() || iteration <= currentTerm || lastLogIndex < state.lastApplied) {
            return ConsensusElectedYou(this.peerId, currentTerm, false)
        }

        currentTerm = iteration
        restartLeaderTimeout()
        return ConsensusElectedYou(this.peerId, currentTerm, true)
    }

    override suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int) {
        if (this.currentTerm > term) {
            logger.info("${this.peerId} - Receive leader elected from peer $peerId for previous term $term, currentTerm: $currentTerm")
            return
        }
        votedFor = VotedFor(peerId, peerAddress)
        logger.info("${this.peerId} - Leader Elected! Is $peerId $peerAddress")
        role = RaftRole.Follower
        signalPublisher.signal(Signal.ConsensusLeaderElected, this, listOf(consensusPeers), null)
        restartLeaderTimeout()
    }

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse {
        val term = heartbeat.term
        val peerId = heartbeat.leaderId
        val acceptedChanges = heartbeat.acceptedChanges.map { it.toLedgerItem() }
        val proposedChanges = heartbeat.proposedChanges.map { it.toLedgerItem() }
        val prevLogIndex = heartbeat.prevLogIndex
        val prevLogTerm = heartbeat.prevLogTerm

        logger.info("${this.peerId} - Received heartbeat $heartbeat")


        if (term < currentTerm) {
            logger.info("${this.peerId} - HandleHeartbeat $peerId $term $currentTerm - true leader is ${votedFor?.id}")
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

        if (prevLogIndex != null && prevLogTerm != null && !state.checkIfItemExist(prevLogIndex, prevLogTerm)) {
            logger.info("${this.peerId} - HandleHeartbeat from $peerId - I don't have some changes need update")
            state.removeNotAcceptedItems(prevLogIndex, prevLogTerm)
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

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
        return ConsensusHeartbeatResponse(true, currentTerm)
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
        val peersWithMessage = consensusPeers.map { peerUrl ->
            Pair(peerUrl, getMessageForPeer(peerUrl))
        }
        val responses = protocolClient.sendConsensusHeartbeat(peersWithMessage)

        val (successResponses, failResponses) = responses
            .filter { it.message != null }
            .partition { it.message!!.success }

        successResponses.map { (peerUrl, heartbeatResponse) ->
            handleSuccessHeartbeatResponseFromPeer(peerUrl)
        }

        logger.info("$peerId - heartbeat responses: \n sucess: $successResponses \n failure: $failResponses")

        val (oldTermResponses, needUpdatesResponses) = failResponses.partition { it.message!!.term != currentTerm }

        needUpdatesResponses.map { (peerUrl, heartbeatResponse) ->
            val oldValues = peerUrlToNextIndex[peerUrl]
            peerUrlToNextIndex[peerUrl] = oldValues
                ?.let { PeerIndexes(it.acceptedIndex - 1, it.acceptedIndex - 1) }
                ?: PeerIndexes()
        }

        val acceptedIndexes: List<Int> = ledgerIndexToMatchIndex
            .filter { (key, value) ->
                checkHalfOfPeerSet(value)
            }
            .map { it.key }

        logger.info("$peerId accept indexes: $acceptedIndexes")
        state.acceptItems(acceptedIndexes)
        acceptedIndexes.forEach { ledgerIndexToMatchIndex.remove(it) }

        if (oldTermResponses.isEmpty()) {
            timer = getLeaderTimer()
            timer.startCounting { sendHeartbeat() }
        } else timer.startCounting {
            val newTerm = oldTermResponses.maxOf { it.message!!.term }
            logger.info("$peerId - some peer is in new term $newTerm, currentTerm $currentTerm\n")
            this.currentTerm = newTerm
            sendLeaderRequest()
        }
    }


    private fun handleSuccessHeartbeatResponseFromPeer(peerUrl: String) {
        val peerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes())
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
            val newPeerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes())
            peerUrlToNextIndex[peerUrl] =
                newPeerIndexes.copy(acceptedIndex = newAcceptedChanges.maxOf { it.ledgerIndex })
        }
    }

    private fun getMessageForPeer(peerUrl: String): ConsensusHeartbeat {
        val peerIndexes = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndexes())
        val newAcceptedChanges = state.getNewAcceptedItems(peerIndexes.acceptedIndex)
        val newProposedChanges = state.getNewProposedItems(peerIndexes.acknowledgedIndex)
        val lastAppliedChangeIdAndTerm = state.getLastAppliedChangeIdAndTermBeforeIndex(peerIndexes.acceptedIndex)


        return ConsensusHeartbeat(
            peerId,
            currentTerm,
            newAcceptedChanges.map { it.toDto() },
            newProposedChanges.map { it.toDto() },
            lastAppliedChangeIdAndTerm.first,
            lastAppliedChangeIdAndTerm.second
        )
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

data class PeerIndexes(val acceptedIndex: Int = -1, val acknowledgedIndex: Int = -1)

data class VotedFor(val id: Int, val address: String)