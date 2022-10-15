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
import java.time.Duration
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import org.slf4j.LoggerFactory
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.*

/** @author Kamil Jarosz */
class RaftConsensusProtocolImpl(
    private val peerId: Int,
    private val peersetId: Int,
    private var peerAddress: String,
    private val ctx: ExecutorCoroutineDispatcher,
    private var consensusPeers: List<String>,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val protocolClient: RaftProtocolClient,
    private val heartbeatTimeout: Duration = Duration.ofSeconds(4),
    private val heartbeatDelay: Duration = Duration.ofMillis(500)
) : ConsensusProtocol<Change, History>, RaftConsensusProtocol, SignalSubject {

    //    TODO: Add map peerIdToUrl
    private var currentTerm: Int = 0
    private val peerUrlToNextIndex: MutableMap<String, PeerIndices> = mutableMapOf()
    private val ledgerIndexToAcceptedNumber: ChangesLedger = ChangesLedger()
    private var votedFor: VotedFor? = null
    private var state: Ledger = Ledger()

    @Volatile
    private var role: RaftRole = RaftRole.Candidate
    private var timer = ProtocolTimerImpl(Duration.ofSeconds(0), Duration.ofSeconds(2), ctx)

    //    DONE: Use only one mutex
    private val mutex = Mutex()

    private var executorService: ExecutorCoroutineDispatcher =
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    override suspend fun begin() {
        logger.info("$peerId - Start raft on address $peerAddress, other peers: $consensusPeers")
        timer.startCounting { sendLeaderRequest() }
    }

    override fun getPeerName() = "peerset${peersetId}/peer${peerId}"

    override fun setOtherPeers(otherPeers: List<String>) {
        logger.info("$peerId - Set new peers: $otherPeers \n old: $consensusPeers")
        if (role == RaftRole.Leader) otherPeers.filter { !consensusPeers.contains(it) }
            .forEach { launchHeartBeatToPeer(it) }
        consensusPeers = otherPeers
    }

    private suspend fun sendLeaderRequest() {
        logger.info("Peer $peerId try to become leader in term $currentTerm")
        signalPublisher.signal(Signal.ConsensusTryToBecomeLeader, this, listOf(consensusPeers), null)
        mutex.withLock {
            currentTerm += 1
            role = RaftRole.Candidate
            votedFor = VotedFor(peerId, peerAddress)
        }

        val responses = protocolClient.sendConsensusElectMe(
            consensusPeers,
            ConsensusElectMe(peerId, currentTerm, state.lastApplied)
        ).map { it.message }

        logger.info("Responses from leader request for $peerId: $responses in iteration $currentTerm")

        val maxTerm = responses.filterNotNull().maxOfOrNull { it.myTerm } ?: currentTerm
        assert(maxTerm >= currentTerm)
        currentTerm = maxTerm

        val positiveResponses = responses.filterNotNull().count { it.voteGranted }

//      TODO: ConsensusPeers should include yourself
        if (!isMoreThanHalf(positiveResponses) || consensusPeers.isEmpty()) {
            mutex.withLock {
                if (role == RaftRole.Candidate) {
                    role = RaftRole.Follower
                    votedFor = null
                }
            }
            restartTimer(RaftRole.Leader)
            return
        }

        logger.info("$peerId - I'm the leader in iteration $currentTerm")

        val leaderAffirmationReactions =
            protocolClient.sendConsensusImTheLeader(
                consensusPeers,
                ConsensusImTheLeader(peerId, peerAddress, currentTerm)
            )


        logger.info("Affirmations responses: $leaderAffirmationReactions")

        mutex.withLock {
            role = RaftRole.Leader
        }

        peerUrlToNextIndex.keys.forEach {
            peerUrlToNextIndex.replace(it, PeerIndices(state.lastApplied, state.lastApplied))
        }

        scheduleHeartbeatToPeers()
    }


    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogIndex: Int): ConsensusElectedYou {
        mutex.lock()
        try {
            logger.info("${this.peerId}/-/${this.peerAddress} - handleRequestVote - ($peerId,$iteration,$lastLogIndex) ($currentTerm,${state.lastApplied})")
            if (amILeader() || iteration <= currentTerm || lastLogIndex < state.lastApplied) {
                return ConsensusElectedYou(this.peerId, currentTerm, false)
            }
            currentTerm = iteration
        } finally {
            mutex.unlock()
        }
        restartTimer()
        return ConsensusElectedYou(this.peerId, currentTerm, true)
    }

    override suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int) {
        if (this.currentTerm > term) {
            logger.info("${this.peerId} - Receive leader elected from peer $peerId for previous term $term, currentTerm: $currentTerm")
            return
        }
        logger.info("${this.peerId} - Leader Elected! Is $peerId $peerAddress")
        mutex.withLock {
            votedFor = VotedFor(peerId, peerAddress, true)
//          TODO: Check if stop() function make sure if you need to wait to all job finish
            if (role == RaftRole.Leader) stop()
            role = RaftRole.Follower
        }
        signalPublisher.signal(Signal.ConsensusLeaderElected, this, listOf(consensusPeers), null)
        restartTimer()
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
        val anyAcceptedChange: Boolean

        mutex.lock()
        try {
            anyAcceptedChange = state.updateLedger(acceptedChanges, proposedChanges)
        } finally {
            mutex.unlock()
        }
        if (anyAcceptedChange) signalPublisher.signal(
            Signal.ConsensusFollowerChangeAccepted,
            this,
            listOf(consensusPeers),
            null,
            acceptedChanges.last().change
        )

        acceptedChanges.lastOrNull()?.let {
            signalPublisher.signal(
                Signal.ConsensusAfterHandlingHeartbeat,
                this,
                listOf(this.consensusPeers),
                null,
                it.change
            )
        }

        restartTimer()
        return ConsensusHeartbeatResponse(true, currentTerm)
    }

    override suspend fun handleProposeChange(change: Change) =
        proposeChange(change)

    override fun setPeerAddress(address: String) {
        this.peerAddress = address
    }

    //    DONE: It should return null if votedFor haven't yet became leader
    override fun getLeaderAddress(): String? = if (votedFor != null && votedFor!!.elected) {
        votedFor!!.address
    } else {
        null
    }

    override fun getProposedChanges(): List<Change> = state.getProposedChanges()
    override fun getAcceptedChanges(): List<Change> = state.getAcceptedChanges()

    private suspend fun sendHeartbeatToPeer(peerUrl: String) {
        val peerWithMessage = Pair(peerUrl, getMessageForPeer(peerUrl))
        val response = protocolClient.sendConsensusHeartbeat(peerWithMessage)
        when {
            response.message == null -> {
                logger.info("Peer doesn't respond $peerUrl")
            }

            response.message.success -> {
                handleSuccessHeartbeatResponseFromPeer(peerUrl)
            }

            response.message.term <= currentTerm -> {
                val oldValues = peerUrlToNextIndex[peerUrl]
//                Done: Add decrement method for PeerIndices
                peerUrlToNextIndex[peerUrl] = oldValues
                    ?.decrement()
                    ?: PeerIndices()
            }

            response.message.term > currentTerm -> stopBeingLeader(response.message.term)
        }

        if (role == RaftRole.Leader && consensusPeers.contains(peerUrl)) launchHeartBeatToPeer(peerUrl)
    }

    //  TODO: Modify this after implementing async propose change
    private suspend fun sendHeartbeat() {
        logger.info("SendHeartbeat from $peerId in $currentTerm to peers: $consensusPeers")
        val peersWithMessage = consensusPeers.map { peerUrl ->
            Pair(peerUrl, getMessageForPeer(peerUrl))
        }
        val responses = protocolClient.sendConsensusHeartbeat(peersWithMessage)

        val (successResponses, failResponses) = responses
            .filter { it.message != null }
            .partition { it.message!!.success }

        successResponses.map { (peerUrl, heartbeatResponse) -> handleSuccessHeartbeatResponseFromPeer(peerUrl) }

        logger.info("$peerId - heartbeat responses: \n sucess: $successResponses \n failure: $failResponses")

        val (oldTermResponses, needUpdatesResponses) = failResponses.partition { it.message!!.term > currentTerm }

        needUpdatesResponses.map { (peerUrl, heartbeatResponse) ->
            val oldValues = peerUrlToNextIndex[peerUrl]
            peerUrlToNextIndex[peerUrl] = oldValues
                ?.let { PeerIndices(it.acceptedIndex - 1, it.acceptedIndex - 1) }
                ?: PeerIndices()
        }

        applyAcceptedChanges()

        if (oldTermResponses.isNotEmpty()) {
            timer.startCounting {
                val newTerm = oldTermResponses.maxOf { it.message!!.term }
                stopBeingLeader(newTerm)
            }
        }
    }

    private fun applyAcceptedChanges() {
//      DONE: change name of ledgerIndexToMatchIndex
        val acceptedIndexes: List<Int> = ledgerIndexToAcceptedNumber.getAcceptedIndexes { isMoreThanHalf(it) }

        logger.info("$peerId accept indexes: $acceptedIndexes")
        state.acceptItems(acceptedIndexes)
        ledgerIndexToAcceptedNumber.removeIndexes(acceptedIndexes)
    }

    private suspend fun stopBeingLeader(newTerm: Int) {
        logger.info("$peerId - some peer is a new leader in new term: $newTerm, currentTerm $currentTerm\n")
//       TODO: fix switch role and add test for it
        this.currentTerm = newTerm
        restartTimer()
    }


    private fun handleSuccessHeartbeatResponseFromPeer(peerUrl: String) {
        val peerIndices = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndices())
        val newAcceptedChanges = state.getNewAcceptedItems(peerIndices.acceptedIndex)
        val newProposedChanges = state.getNewProposedItems(peerIndices.acknowledgedIndex)

        if (newProposedChanges.isNotEmpty()) {
            newProposedChanges.forEach {
//              Done: increment number of peers which accept this ledgerIndex
                ledgerIndexToAcceptedNumber.incrementKey(it.ledgerIndex)
            }
            peerUrlToNextIndex[peerUrl] =
                peerIndices.copy(acknowledgedIndex = newProposedChanges.maxOf { it.ledgerIndex })
        }

        if (newAcceptedChanges.isNotEmpty()) {
            val newPeerIndices = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndices())
            peerUrlToNextIndex[peerUrl] =
                newPeerIndices.copy(acceptedIndex = newAcceptedChanges.maxOf { it.ledgerIndex })
        }

        applyAcceptedChanges()
    }

    private fun getMessageForPeer(peerUrl: String): ConsensusHeartbeat {
        val peerIndices = peerUrlToNextIndex.getOrDefault(peerUrl, PeerIndices())
        val newAcceptedChanges = state.getNewAcceptedItems(peerIndices.acceptedIndex)
        val newProposedChanges = state.getNewProposedItems(peerIndices.acknowledgedIndex)
        val lastAppliedChangeIdAndTerm = state.getLastAppliedChangeIdAndTermBeforeIndex(peerIndices.acceptedIndex)

        return ConsensusHeartbeat(
            peerId,
            currentTerm,
            newAcceptedChanges.map { it.toDto() },
            newProposedChanges.map { it.toDto() },
            lastAppliedChangeIdAndTerm.first,
            lastAppliedChangeIdAndTerm.second
        )
    }

    private suspend fun restartTimer(role: RaftRole = RaftRole.Follower) {
        timer.cancelCounting()
        val text = when (role) {
//            TODO: Think about this case again
            RaftRole.Leader -> {
                timer = getLeaderTimer(heartbeatDelay)
                "Some peer tried to become leader,"
            }

            RaftRole.Follower -> {
                timer = getHeartbeatTimer()
                "$peerId/-/$peerAddress - leader ${votedFor?.id}/-/${votedFor?.address}  doesn't send heartbeat, start try to become leader"
            }

            else -> {
                throw AssertionError()
            }
        }
        timer.startCounting {
            logger.info(text)
            sendLeaderRequest()
        }
    }

    //  TODO: sync change will have to use Condition/wait/notifyAll
    private suspend fun proposeChangeToLedger(changeWithAcceptNum: Change): ConsensusResult {

//      TODO: it will be changed
        if (state.changeAlreadyProposed(changeWithAcceptNum)) {
            return ConsensusChangeAlreadyProposed
        }
        timer.cancelCounting()
        logger.info("Propose change to ledger: $changeWithAcceptNum")
        val id = state.proposeChange(changeWithAcceptNum, currentTerm)

        ledgerIndexToAcceptedNumber.initializeKey(id)
//      Propose change
        sendHeartbeat()
        signalPublisher.signal(Signal.ConsensusAfterProposingChange, this, listOf(consensusPeers), null)

        if (state.lastApplied != id) return ConsensusResultUnknown
        timer.cancelCounting()
        logger.info("Change accepted and propagate it: $changeWithAcceptNum")
//      If change accepted, propagate it
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
        ConsensusResult.valueOf(response)
    } catch (e: Exception) {
        logger.info("$peerId - request to leader (${votedFor!!.address}) failed with exception: $e")
        ConsensusFailure
    }

    private suspend fun tryToProposeChangeMyself(changeWithAcceptNum: Change): ConsensusResult {
        val id = state.proposeChange(changeWithAcceptNum, currentTerm)
        ledgerIndexToAcceptedNumber.initializeKey(id)
        timer.startCounting {
            logger.info("$peerId - change was proposed a no leader is elected")
            sendLeaderRequest()
        }
        return ConsensusSuccess
    }

    //   TODO: only one change can be proposed at the same time
    override suspend fun proposeChange(change: Change): ConsensusResult {
        logger.info("$peerId received change: $change")

        mutex.lock()
        try {
            return when {
                amILeader() -> proposeChangeToLedger(change)
                votedFor != null -> sendRequestToLeader(change)
//              TODO: Change after queue
                else -> tryToProposeChangeMyself(change)
            }
        } finally {
            mutex.unlock()
        }
    }

    private fun scheduleHeartbeatToPeers() {
        consensusPeers.forEach {
            launchHeartBeatToPeer(it)
        }
    }

    private fun launchHeartBeatToPeer(peerUrl: String): Job =
        with(CoroutineScope(executorService)) {
            launch {
                delay(heartbeatDelay.toMillis())
                sendHeartbeatToPeer(peerUrl)
            }
        }


    override fun getState(): History {
        val history = state.getHistory()
        logger.info("$peerId - request for state: $history")
        return history
    }

    override fun stop() {
        executorService.cancel()
//        executorService.close()
    }

    //    TODO: unit tests for this function
    private fun isMoreThanHalf(value: Int): Boolean = value * 2 > consensusPeers.size

    private fun amILeader(): Boolean = role == RaftRole.Leader

    private fun getLeaderTimer(backoff: Duration = Duration.ZERO) = ProtocolTimerImpl(heartbeatDelay, backoff, ctx)
    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)

    companion object {
        private val logger = LoggerFactory.getLogger(RaftConsensusProtocolImpl::class.java)
    }


}

data class PeerIndices(val acceptedIndex: Int = -1, val acknowledgedIndex: Int = -1) {
    public fun decrement(): PeerIndices = PeerIndices(acceptedIndex - 1, acceptedIndex - 1)
}

data class VotedFor(val id: Int, val address: String, val elected: Boolean = false)