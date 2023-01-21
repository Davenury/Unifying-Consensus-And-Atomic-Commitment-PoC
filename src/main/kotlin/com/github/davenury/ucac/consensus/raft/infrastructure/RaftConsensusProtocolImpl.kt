package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.*
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors


/** @author Kamil Jarosz */
class RaftConsensusProtocolImpl(
    private val history: History,
    private var peerAddress: String,
    private val ctx: ExecutorCoroutineDispatcher,
    private var peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val protocolClient: RaftProtocolClient,
    private val heartbeatTimeout: Duration = Duration.ofSeconds(4),
    private val heartbeatDelay: Duration = Duration.ofMillis(500),
    private val transactionBlocker: TransactionBlocker
) : ConsensusProtocol, RaftConsensusProtocol, SignalSubject {
    private val globalPeerId = peerResolver.currentPeer()
    private val peerId = globalPeerId.peerId

    private var currentTerm: Int = 0
    private val peerUrlToNextIndex: MutableMap<GlobalPeerId, PeerIndices> = mutableMapOf()
    private val voteContainer: VoteContainer = VoteContainer()
    private var votedFor: VotedFor? = null
    private var state: Ledger = Ledger(history)

    @Volatile
    private var role: RaftRole = RaftRole.Candidate
    private var timer = ProtocolTimerImpl(Duration.ofSeconds(0), Duration.ofSeconds(2), ctx)
    private var lastHeartbeatTime = Instant.now()

    //    DONE: Use only one mutex
    private val mutex = Mutex()
    private var executorService: ExecutorCoroutineDispatcher? = null
    private val leaderRequestExecutorService = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    //    TODO: Useless, it should use a worker queue.
    private val queuedChanges: MutableList<Change> = mutableListOf()

    private fun otherConsensusPeers(): List<PeerAddress> {
        return peerResolver.getPeersFromCurrentPeerset().filter { it.globalPeerId != globalPeerId }
    }

    override suspend fun begin() {
        logger.info("Starting raft on address $peerAddress, other peers: ${otherConsensusPeers()}")
        timer.startCounting { sendLeaderRequest() }
    }

    override fun getPeerName() = globalPeerId.toString()

    private suspend fun sendLeaderRequest() {
        if (executorService != null) {
            restartTimer(RaftRole.Candidate)
            throw Exception("$globalPeerId Try become leader before cleaning")
        }
        signalPublisher.signal(
            Signal.ConsensusTryToBecomeLeader,
            this,
            listOf(otherConsensusPeers()),
            null
        )
        mutex.withLock {
            currentTerm += 1
            role = RaftRole.Candidate
            votedFor = VotedFor(peerId, peerAddress)
        }

        logger.info("Trying to become a leader in term $currentTerm")

        val responses = protocolClient.sendConsensusElectMe(
            otherConsensusPeers(),
            ConsensusElectMe(peerId, currentTerm, state.commitIndex)
        ).map { it.message }

        logger.debug("Responses from leader request: $responses in iteration $currentTerm")

        val maxTerm = responses.filterNotNull().maxOfOrNull { it.myTerm } ?: currentTerm
        assert(maxTerm >= currentTerm)
        currentTerm = maxTerm

        val positiveResponses = responses.filterNotNull().count { it.voteGranted }

//      DONE: ConsensusPeers should include yourself -> renamed to otherConsensusPeers
        if (!isMoreThanHalf(positiveResponses) || otherConsensusPeers().isEmpty()) {
            mutex.withLock {
                if (role == RaftRole.Candidate) {
                    role = RaftRole.Follower
                    votedFor = null
                    restartTimer(RaftRole.Candidate)
                }
            }
            return
        }

        mutex.withLock {
            role = RaftRole.Leader
            votedFor = votedFor!!.copy(elected = true)
            assert(executorService == null)
            executorService = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        }

        logger.info("I have been selected as a leader (in term $currentTerm)")

        val leaderAffirmationReactions =
            protocolClient.sendConsensusImTheLeader(
                otherConsensusPeers(),
                ConsensusImTheLeader(peerId, peerAddress, currentTerm)
            )

        logger.debug("Affirmations responses: $leaderAffirmationReactions")

        peerUrlToNextIndex.keys.forEach {
            peerUrlToNextIndex.replace(it, PeerIndices(state.lastApplied, state.lastApplied))
        }

        scheduleHeartbeatToPeers()
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogIndex: Int): ConsensusElectedYou {
        logger.debug("Handling vote request: peerId=$peerId,iteration=$iteration,lastLogIndex=$lastLogIndex, currentTerm=$currentTerm,lastApplied=${state.lastApplied}")
        mutex.withLock {
            val denyVoteResponse = ConsensusElectedYou(this.peerId, currentTerm, false)
            if (iteration <= currentTerm) {
                logger.info("Denying vote for $peerId due to an old term ($iteration vs $currentTerm), I voted for ${votedFor?.id ?: "null"}")
                return denyVoteResponse
            }
            if (amILeader()) stopBeingLeader(iteration)

            if (lastLogIndex < state.commitIndex && amILeader()) {
                logger.info("Denying vote for $peerId due to an old index ($lastLogIndex vs ${state.commitIndex})")
                return denyVoteResponse
            }
            currentTerm = iteration
        }
        restartTimer()
        logger.info("Voted for $peerId in term $iteration")
        return ConsensusElectedYou(this.peerId, currentTerm, true)
    }

    override suspend fun handleLeaderElected(peerId: Int, peerAddress: String, term: Int) {
        if (this.currentTerm > term) {
            logger.info("Received leader elected from peer $peerId for previous term $term, currentTerm: $currentTerm")
            return
        }
        restartTimer()
        logger.info("A leader has been elected: $peerId (in term $term)")
        mutex.withLock {
            votedFor = VotedFor(peerId, peerAddress, true)
//          DONE: Check if stop() function make sure if you need to wait to all job finish ->
//          fixed by setting executor to null and adding null condition in if
            if (role == RaftRole.Leader) stopBeingLeader(term)
            role = RaftRole.Follower
            currentTerm = term
        }
        signalPublisher.signal(
            Signal.ConsensusLeaderElected,
            this,
            listOf(otherConsensusPeers()),
            null
        )

        releaseBlockerFromPreviousTermChanges()
    }

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse {
        val term = heartbeat.term
        val acceptedChanges = heartbeat.acceptedChanges.map { it.toLedgerItem() }
        val proposedChanges = heartbeat.proposedChanges.map { it.toLedgerItem() }
        val prevLogIndex = heartbeat.prevLogIndex
        val prevLogTerm = heartbeat.prevLogTerm

//        logger.debug("Received heartbeat $heartbeat")
        logger.info("Received heartbeat $heartbeat")
        logger.debug("I have in state: ${state.acceptedItems.map { it.ledgerIndex }} ${state.proposedItems.map { it.ledgerIndex }} \n and should have: $prevLogIndex, message changes: ${acceptedChanges.map { it.ledgerIndex }} ${proposedChanges.map { it.ledgerIndex }}")


        if (term < currentTerm) {
            logger.info("The received heartbeat has an old term ($term vs $currentTerm)")
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

        if (history.getCurrentEntry().getId() != heartbeat.currentHistoryEntryId && acceptedChanges.isEmpty()) {
            logger.info("Received heartbeat from leader that has outdated history")
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

//      Restart timer because we received heartbeat from proper leader

        if (currentTerm < term) {
            mutex.withLock {
                currentTerm = term
            }
            releaseBlockerFromPreviousTermChanges()
        }

        mutex.withLock {
            if (role == RaftRole.Leader) {
                stopBeingLeader(term)
                role = RaftRole.Follower
            }
            lastHeartbeatTime = Instant.now()
        }
        restartTimer()

        val haveAllPreviousChanges = if (prevLogIndex == null || prevLogTerm == null) true
        else {
            state.checkIfItemExist(
                prevLogIndex,
                prevLogTerm
            ) || acceptedChanges.any { it.ledgerIndex == prevLogIndex && it.term == prevLogTerm }
        }

        if (!haveAllPreviousChanges) {
            logger.info("The received heartbeat is missing some changes (I am behind)")
            state.removeNotAcceptedItems(prevLogIndex!!, prevLogTerm!!)
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

        val areProposedChangesIncompatible =
            proposedChanges.isNotEmpty() && proposedChanges.any { !history.isEntryCompatible(it.entry) }

        when {
            transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() != ProtocolName.CONSENSUS ->
                return ConsensusHeartbeatResponse(false, currentTerm, true)

            acceptedChanges.isNotEmpty() && transactionBlocker.isAcquired() && transactionBlocker.getChangeId() == proposedChanges.first().changeId -> {
                logger.debug("Received again the same proposedChanges")
                updateLedger(heartbeat, acceptedChanges)
                return ConsensusHeartbeatResponse(true, currentTerm)
            }

            acceptedChanges.isNotEmpty() && transactionBlocker.isAcquired() -> {
                logger.debug("Received heartbeat when is blocked so only accepted changes")
                updateLedger(heartbeat, acceptedChanges)
                return ConsensusHeartbeatResponse(true, currentTerm, true)
            }

            proposedChanges.isNotEmpty() && transactionBlocker.isAcquired() -> {
                logger.debug("Received heartbeat but is blocked, so can't accept proposed changes")
                return ConsensusHeartbeatResponse(false, currentTerm, true)
            }

            acceptedChanges.isNotEmpty() && areProposedChangesIncompatible -> {
                logger.debug("Received heartbeat but changes are incompatible, updated accepted changes")
                updateLedger(heartbeat, acceptedChanges)
                return ConsensusHeartbeatResponse(true, currentTerm, incompatibleWithHistory = true)
            }

            areProposedChangesIncompatible -> {
                logger.debug("Received heartbeat but changes are incompatible")
                return ConsensusHeartbeatResponse(false, currentTerm, incompatibleWithHistory = true)
            }

            proposedChanges.isNotEmpty() ->
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, proposedChanges.first().changeId)
        }

        updateLedger(heartbeat, acceptedChanges, proposedChanges)

        return ConsensusHeartbeatResponse(true, currentTerm)
    }

    private suspend fun updateLedger(
        heartbeat: ConsensusHeartbeat,
        acceptedChanges: List<LedgerItem>,
        proposedChanges: List<LedgerItem> = listOf()
    ) {
        val updateResult: LedgerUpdateResult
        mutex.withLock {
            updateResult = state.updateLedger(acceptedChanges, proposedChanges)
        }

        updateResult.acceptedItems.forEach { acceptedItem ->
            signalPublisher.signal(
                signal = Signal.ConsensusFollowerChangeAccepted,
                subject = this,
                peers = listOf(otherConsensusPeers()),
                change = Change.fromHistoryEntry(acceptedItem.entry),
                historyEntry = acceptedItem.entry,
            )
        }

        updateResult.proposedItems.forEach { proposedItem ->
            signalPublisher.signal(
                signal = Signal.ConsensusFollowerChangeProposed,
                subject = this,
                peers = listOf(otherConsensusPeers()),
                change = Change.fromHistoryEntry(proposedItem.entry),
                historyEntry = proposedItem.entry
            )
        }

        updateResult.acceptedItems.forEach {
            changeIdToCompletableFuture[it.changeId]?.complete(ChangeResult(ChangeResult.Status.SUCCESS))
        }

        val message = "Received heartbeat from ${heartbeat.leaderId} with " +
                "${updateResult.proposedItems.size} proposed and " +
                "${updateResult.acceptedItems.size} accepted items"
        logger.debug(message)

        if (updateResult.proposedItems.isNotEmpty() || updateResult.acceptedItems.isNotEmpty()) {
            logger.info(message)
        }

        if (updateResult.acceptedItems.isNotEmpty()) {
            println("updateLedger tryToReleaseBlocker")
            transactionBlocker.tryToReleaseBlockerChange(
                ProtocolName.CONSENSUS,
                updateResult.acceptedItems.first().changeId
            )
        }
    }


    override suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult> =
        proposeChangeAsync(change)

    override fun setPeerAddress(address: String) {
        this.peerAddress = address
    }

    //    DONE: It should return null if votedFor haven't yet became leader
    override suspend fun getLeaderAddress(): String? =
        mutex.withLock {
            return if (votedFor != null && votedFor!!.elected) {
                votedFor!!.address
            } else {
                null
            }
        }

    override suspend fun getProposedChanges(): List<Change> = state.getProposedItems()
        .mapNotNull { Change.fromHistoryEntry(it.entry) }


    override suspend fun getAcceptedChanges(): List<Change> = state.getAcceptedItems()
        .mapNotNull { Change.fromHistoryEntry(it.entry) }

    private suspend fun sendHeartbeatToPeer(peer: GlobalPeerId) {
        val peerAddress = peerResolver.resolve(peer)
        val peerMessage = getMessageForPeer(peerAddress)
        val response = protocolClient.sendConsensusHeartbeat(peerAddress, peerMessage)

        // We should schedule heartbeat even if something failed during handling response
        if (role == RaftRole.Leader
            && otherConsensusPeers().any { it.globalPeerId == peer }
            && executorService != null
        ) {
            launchHeartBeatToPeer(peer)
        }

        when {
            response.message == null -> {
                logger.info("Peer $peer did not respond to heartbeat ${peerAddress.globalPeerId}")
            }

            response.message.transactionBlocked && response.message.success -> {
                logger.info("Peer $peer has transaction blocker on but accepted changes was applied")
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.acceptedChanges)
            }

            response.message.success && response.message.incompatibleWithHistory -> {
                logger.info("Peer $peer's history is incompatible with proposed change but accepted some changes so retry proposing")
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.acceptedChanges)
            }

            response.message.transactionBlocked -> {
                logger.info("Peer $peer has transaction blocker on")
            }

            response.message.incompatibleWithHistory -> {
                logger.info("Peer $peer's history is incompatible with proposed change")
                state.checkIfProposedItemsAreStillValid()
            }

            response.message.success -> {
                logger.debug("Heartbeat sent successfully to ${peerAddress.globalPeerId}")
                handleSuccessHeartbeatResponseFromPeer(
                    peerAddress,
                    peerMessage.acceptedChanges,
                    peerMessage.proposedChanges
                )
            }

            response.message.term > currentTerm -> {
                logger.info("Based on info from $peer, someone else is currently leader")
                stopBeingLeader(response.message.term)
            }


            response.message.term <= currentTerm -> {
                logger.info("Peer ${peerAddress.globalPeerId} is not up to date, decrementing index")
                val oldValues = peerUrlToNextIndex[peerAddress.globalPeerId]
//                Done: Add decrement method for PeerIndices
                peerUrlToNextIndex[peerAddress.globalPeerId] = oldValues
                    ?.decrement()
                    ?: PeerIndices()
            }
        }

        checkIfQueuedChanges()
    }

    private suspend fun applyAcceptedChanges() {
//      DONE: change name of ledgerIndexToMatchIndex
        val acceptedIndexes: List<Int> = voteContainer.getAcceptedChanges { isMoreThanHalf(it) }

        val acceptedItems = state.getProposedItems(acceptedIndexes)
        if (acceptedItems.isNotEmpty()) {
            logger.info("Applying accepted changes: $acceptedItems")
            println("applyAcceptedChanges tryToReleaseBlocker")
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, acceptedItems.first().changeId)
        }

        acceptedItems.forEach {
            signalPublisher.signal(
                signal = Signal.ConsensusAfterProposingChange,
                subject = this,
                peers = listOf(otherConsensusPeers()),
                change = Change.fromHistoryEntry(it.entry),
                historyEntry = it.entry,
            )
        }

        state.acceptItems(acceptedIndexes)
        voteContainer.removeChanges(acceptedIndexes)

        acceptedItems.forEach {
            changeIdToCompletableFuture.putIfAbsent(it.changeId, CompletableFuture())
        }

        acceptedItems
            .map { changeIdToCompletableFuture[it.changeId] }
            .forEach { it!!.complete(ChangeResult(ChangeResult.Status.SUCCESS)) }
    }

    private suspend fun stopBeingLeader(newTerm: Int) {
        logger.info("Some peer is a new leader in new term: $newTerm, currentTerm $currentTerm")
//       TODO: fix switch role and add test for it
        this.currentTerm = newTerm
        stopExecutorService()
        restartTimer()
    }


    //  TODO: Useless function
    private suspend fun checkIfQueuedChanges() {
        if (queuedChanges.isEmpty()) return
        val change = queuedChanges.removeAt(0)
        proposeChangeToLedger(changeIdToCompletableFuture[change.id]!!, change)
    }

    private suspend fun handleSuccessHeartbeatResponseFromPeer(
        peerAddress: PeerAddress,
        newAcceptedChanges: List<LedgerItemDto>,
        newProposedChanges: List<LedgerItemDto> = listOf()
    ) {
        val globalPeerId = peerAddress.globalPeerId

        val peerIndices = peerUrlToNextIndex.getOrDefault(globalPeerId, PeerIndices())

        if (newProposedChanges.isNotEmpty()) {
            newProposedChanges.forEach {
//              Done: increment number of peers which accept this ledgerIndex
                voteContainer.voteForChange(it.ledgerIndex)
            }
            peerUrlToNextIndex[globalPeerId] =
                peerIndices.copy(acknowledgedIndex = newProposedChanges.maxOf { it.ledgerIndex })
        }

        if (newAcceptedChanges.isNotEmpty()) {
            val newPeerIndices = peerUrlToNextIndex.getOrDefault(globalPeerId, PeerIndices())
            peerUrlToNextIndex[globalPeerId] =
                newPeerIndices.copy(acceptedIndex = newAcceptedChanges.maxOf { it.ledgerIndex })
        }

        applyAcceptedChanges()
    }

    private suspend fun getMessageForPeer(peerAddress: PeerAddress): ConsensusHeartbeat {
        val peerIndices = peerUrlToNextIndex.getOrDefault(peerAddress.globalPeerId, PeerIndices()) // TODO
        val newAcceptedChanges = state.getNewAcceptedItems(peerIndices.acceptedIndex)
        val newProposedChanges = state.getNewProposedItems(peerIndices.acknowledgedIndex)
        val lastAppliedChangeIdAndTerm = state.getLastAppliedChangeIdAndTermBeforeIndex(peerIndices.acceptedIndex)

        return ConsensusHeartbeat(
            peerId,
            currentTerm,
            newAcceptedChanges.map { it.toDto() },
            newProposedChanges.map { it.toDto() },
            lastAppliedChangeIdAndTerm?.first,
            lastAppliedChangeIdAndTerm?.second,
            history.getCurrentEntry().getId()
        )
    }

    private suspend fun restartTimer(role: RaftRole = RaftRole.Follower) {

        timer.cancelCounting()
        val text = when (role) {
//            DONE: Think about this case again -> changed to candidate
            RaftRole.Candidate -> {
                timer = getHeartbeatTimer()
                "Leader not chosen make a timeout and try once again"
            }

            RaftRole.Follower -> {
                timer = getHeartbeatTimer()
                "leader ${votedFor?.id}/-/${votedFor?.address}  doesn't send heartbeat, start try to become leader"
            }

            else -> {
                restartTimer()
                throw AssertionError("Leader restarted timer")
            }
        }

        timer.startCounting {

            val differenceFromLastHeartbeat: Duration
            mutex.withLock {
                differenceFromLastHeartbeat = Duration.between(lastHeartbeatTime, Instant.now())
            }
            if (differenceFromLastHeartbeat > heartbeatTimeout) {
                signalPublisher.signal(
                    Signal.ConsensusLeaderDoesNotSendHeartbeat,
                    this,
                    listOf(otherConsensusPeers()),
                    null
                )
                logger.info(text)
                sendLeaderRequest()
            } else {
                restartTimer(this.role)
            }
        }
    }

    //  TODO: sync change will have to use Condition/wait/notifyAll
    private suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change) {
        var entry = change.toHistoryEntry(globalPeerId.peersetId)
        changeIdToCompletableFuture[change.id] = result

        val id: Int
        mutex.withLock {
            if (state.entryAlreadyProposed(entry)) {
                logger.info("Already proposed that change: $change")
                return
            }

            if (transactionBlocker.isAcquired() || isDuring2PAndChangeDoesntFinishIt(change)) {
                val msg =
                    if (transactionBlocker.isAcquired()) "transaction is blocked"
                    else "is during 2PC"
                logger.info(
                    "Queued change, because: $msg"
                )
                queuedChanges.add(change)
                return
            }

            try {
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, change.id)
            } catch (ex: AlreadyLockedException) {
                logger.info("Is already blocked on other transaction ${transactionBlocker.getProtocolName()}")
                result.complete(ChangeResult(ChangeResult.Status.CONFLICT))
                throw ex
            }

            val updatedChange: Change


            if (checkTwoPCHistoryCompatibility(change)) {
                val peersetId = peerResolver.currentPeer().peersetId
                updatedChange = change.copyWithNewParentId(peersetId, history.getCurrentEntry().getId())
                entry = updatedChange.toHistoryEntry(peersetId)
            } else {
                updatedChange = change
            }



            if (!history.isEntryCompatible(entry)) {
                logger.info(
                    "Proposed change is incompatible. \n CurrentChange: ${
                        history.getCurrentEntry().getId()
                    } \n Change.parentId: ${
                        updatedChange.toHistoryEntry(peerResolver.currentPeer().peersetId).getParentId()
                    }"
                )
                result.complete(ChangeResult(ChangeResult.Status.CONFLICT))
                println("proposeChangeToLedger tryToReleaseBlocker")
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, change.id)
                return
            }

            logger.info("Propose change to ledger: $updatedChange")
            id = state.proposeEntry(entry, currentTerm, updatedChange.id)
            voteContainer.initializeChange(id)
        }
    }

    private fun isDuring2PAndChangeDoesntFinishIt(change: Change): Boolean {
        val currentHistoryChange = history
            .getCurrentEntry()
            .let { Change.fromHistoryEntry(it) }

        val isDuring2PC: Boolean = currentHistoryChange
            ?.let { it is TwoPCChange && it.twoPCStatus == TwoPCStatus.ACCEPTED }
            ?: false

        val changeIsAbortingOf2PC: Boolean =
            change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED

        val changeIsApplyingOf2PC: Boolean =
            currentHistoryChange is TwoPCChange && change == currentHistoryChange.change.copyWithNewParentId(
                peerResolver.currentPeer().peersetId,
                history.getCurrentEntry().getId(),
            )

        return isDuring2PC && !(changeIsAbortingOf2PC || changeIsApplyingOf2PC)
    }

    private fun checkTwoPCHistoryCompatibility(change: Change): Boolean {
        val currentEntry = history.getCurrentEntry()

        val grandParentChange: Change =
            history.getEntryFromHistory(currentEntry.getParentId() ?: return false)
                ?.let { Change.fromHistoryEntry(it) }
                ?: return false

        if (grandParentChange !is TwoPCChange) return false

        val peersetId = peerResolver.currentPeer().peersetId


        return grandParentChange.change.toHistoryEntry(peersetId).getId() == change.toHistoryEntry(peersetId)
            .getParentId()
    }

    private fun releaseBlockerFromPreviousTermChanges() {
        if (transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() == ProtocolName.CONSENSUS && transactionBlocker.getChangeId() == state.proposedItems.last().changeId) {
            logger.info("Release blocker for changes from previous term")
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, state.proposedItems.last().changeId)

            changeIdToCompletableFuture[state.proposedItems.last().changeId]
                ?.complete(ChangeResult(ChangeResult.Status.TIMEOUT))
        }
    }


    private suspend fun sendRequestToLeader(cf: CompletableFuture<ChangeResult>, change: Change) {
        with(CoroutineScope(leaderRequestExecutorService)) {
            launch(MDCContext()) {
                val result: ChangeResult = try {
                    val response =
                        httpClient.post<ChangeResult>("http://${votedFor!!.address}/consensus/request_apply_change") {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body = change
                        }
                    logger.info("Response from leader: $response")
                    response
                } catch (e: Exception) {
                    logger.info("Request to leader (${votedFor!!.address}) failed", e)
                    null
                } ?: ChangeResult(ChangeResult.Status.TIMEOUT)

                if (result.status == ChangeResult.Status.CONFLICT) {
                    cf.complete(result)
                }
            }
        }
    }

    //   TODO: only one change can be proposed at the same time
    @Deprecated("use proposeChangeAsync")
    override suspend fun proposeChange(change: Change): ChangeResult = proposeChangeAsync(change).await()

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val result = CompletableFuture<ChangeResult>()
        changeIdToCompletableFuture[change.id] = result
        when {
            amILeader() -> {
                logger.info("Proposing change: $change")
                proposeChangeToLedger(result, change)
            }

            votedFor != null -> {
                logger.info("Forwarding change to the leader(${votedFor!!}): $change")
                sendRequestToLeader(result, change)
            }
//              TODO: Change after queue
            else -> {
                result.complete(ChangeResult(ChangeResult.Status.TIMEOUT, "There should be always a leader"))
                throw Exception("There should be always a leader")
            }
        }
        return result
    }

    private fun scheduleHeartbeatToPeers() {
        otherConsensusPeers().forEach {
            launchHeartBeatToPeer(it.globalPeerId)
        }
    }

    private fun launchHeartBeatToPeer(peer: GlobalPeerId): Job =
        with(CoroutineScope(executorService!!)) {
            launch(MDCContext()) {
                delay(heartbeatDelay.toMillis())
                sendHeartbeatToPeer(peer)
            }
        }

    override fun getState(): History {
        val history: History
        runBlocking {
            mutex.withLock {
                history = state.getHistory()
                logger.info("Request for state: $history")
            }
        }
        return history
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override fun stop() {
        logger.info("Stop whole consensus")
        stopExecutorService()
        leaderRequestExecutorService.cancel()
        leaderRequestExecutorService.close()
    }

    private fun stopExecutorService() {
        executorService?.cancel()
        executorService?.close()
        executorService = null
    }

    //    DONE: unit tests for this function
    fun isMoreThanHalf(value: Int): Boolean = (value + 1) * 2 > otherConsensusPeers().size + 1

    private fun amILeader(): Boolean = role == RaftRole.Leader

    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)

    companion object {
        private val logger = LoggerFactory.getLogger("raft")
    }
}

data class PeerIndices(val acceptedIndex: Int = -1, val acknowledgedIndex: Int = -1) {
    fun decrement(): PeerIndices = PeerIndices(acceptedIndex - 1, acceptedIndex - 1)
}

data class VotedFor(val id: Int, val address: String, val elected: Boolean = false)
