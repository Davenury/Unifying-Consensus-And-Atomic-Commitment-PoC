package com.github.davenury.ucac.consensus.raft.infrastructure

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.*
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
    private val transactionBlocker: TransactionBlocker,
    private val isMetricTest: Boolean
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
            mutex.withLock {
                restartTimer(RaftRole.Candidate)
            }
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

        if (!isMoreThanHalf(positiveResponses) || otherConsensusPeers().isEmpty()) {
            mutex.withLock {
                restartTimer(RaftRole.Candidate)
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

        peerUrlToNextIndex.keys.forEach {
            peerUrlToNextIndex.replace(it, PeerIndices(state.lastApplied, state.lastApplied))
        }

        scheduleHeartbeatToPeers()
    }

    override suspend fun handleRequestVote(peerId: Int, iteration: Int, lastLogId: String): ConsensusElectedYou {
        logger.debug(
            "Handling vote request: peerId=$peerId,iteration=$iteration,lastLogIndex=$lastLogId, " +
                    "currentTerm=$currentTerm,lastApplied=${state.lastApplied}"
        )

        mutex.withLock {
            val denyVoteResponse = ConsensusElectedYou(this.peerId, currentTerm, false)
            if (iteration < currentTerm || (iteration == currentTerm && votedFor != null)) {
                logger.info(
                    "Denying vote for $peerId due to an old term ($iteration vs $currentTerm), " +
                            "I voted for ${votedFor?.id}"
                )
                return denyVoteResponse
            }
            if (amILeader()) stopBeingLeader(iteration)

            val candidateIsUpToDate: Boolean = lastLogId == state.commitIndex || !state.checkIfItemExist(lastLogId)

            if (!candidateIsUpToDate && amILeader()) {
                logger.info("Denying vote for $peerId due to an old index ($lastLogId vs ${state.commitIndex})")
                return denyVoteResponse
            }
            currentTerm = iteration
            val peerAddress = peerResolver.resolve(GlobalPeerId(globalPeerId.peersetId, peerId)).address
            votedFor = VotedFor(peerId, peerAddress)
            restartTimer()
        }
        logger.info("Voted for $peerId in term $iteration")
        return ConsensusElectedYou(this.peerId, currentTerm, true)
    }

    private suspend fun newLeaderElected(leaderId: Int, term: Int) {
        logger.info("A leader has been elected: $leaderId (in term $term)")
        val leaderAddress = peerResolver.resolve(GlobalPeerId(globalPeerId.peersetId, leaderId)).address
        votedFor = VotedFor(peerId, leaderAddress, true)
//          DONE: Check if stop() function make sure if you need to wait to all job finish ->
//          fixed by setting executor to null and adding null condition in if
        if (role == RaftRole.Leader) stopBeingLeader(term)
        role = RaftRole.Follower
        currentTerm = term
        signalPublisher.signal(
            Signal.ConsensusLeaderElected,
            this,
            listOf(otherConsensusPeers()),
            null
        )

        Metrics.refreshLastHeartbeat()

        releaseBlockerFromPreviousTermChanges()

        state.removeNotAcceptedItems()

    }

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse = mutex.withLock {
        Metrics.registerTimerHeartbeat()
        val term = heartbeat.term
        val leaderCommitId = heartbeat.leaderCommitId
        val isUpdatedCommitIndex = state.isNotApplied(leaderCommitId)
        val proposedChanges = heartbeat.logEntries.map { it.toLedgerItem() }
        val prevEntryId = heartbeat.prevEntryId

//        logger.info("Received heartbeat isUpdatedCommitIndex $isUpdatedCommitIndex")
//        logger.info("Lastapplied: ${state.lastApplied} commitIndex: ${state.commitIndex}")
//        logger.info("PrevEntry: ${prevEntryId},  leaderCommitId: ${leaderCommitId} logEntries: ${proposedChanges.map { it.entry.getId() }}")

        if (term < currentTerm) {
            logger.info("The received heartbeat has an old term ($term vs $currentTerm)")
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

        if (currentTerm < term || votedFor == null || votedFor?.elected == false)
            newLeaderElected(heartbeat.leaderId, term)

        if (history.getCurrentEntry().getId() != heartbeat.currentHistoryEntryId && !isUpdatedCommitIndex) {
            logger.info("Received heartbeat from leader that has outdated history ${state.commitIndex}  $leaderCommitId")
            return ConsensusHeartbeatResponse(false, currentTerm)
        }

//      Restart timer because we received heartbeat from proper leader
        if (currentTerm < term) {
            currentTerm = term
            releaseBlockerFromPreviousTermChanges()
        }

        lastHeartbeatTime = Instant.now()

        restartTimer()

        val notAppliedProposedChanges = proposedChanges.filter { state.isNotAppliedOrProposed(it.entry.getId()) }

        val prevLogEntryExists = prevEntryId == null || state.checkIfItemExist(prevEntryId)

        val commitIndexEntryExists = notAppliedProposedChanges
            .find { it.entry.getId() == leaderCommitId }
            ?.let { true }
            ?: state.checkIfItemExist(leaderCommitId)


        val areProposedChangesIncompatible =
            notAppliedProposedChanges.isNotEmpty() && notAppliedProposedChanges.any { !history.isEntryCompatible(it.entry) }

        val acceptedChangesFromProposed = notAppliedProposedChanges
            .indexOfFirst { it.entry.getId() == leaderCommitId }
            .let { notAppliedProposedChanges.take(it + 1) }


        when {
            !commitIndexEntryExists -> {
                logger.info("I miss some entries to commit (I am behind)")
                return ConsensusHeartbeatResponse(false, currentTerm)
            }

            !prevLogEntryExists -> {
                logger.info("The received heartbeat is missing some changes (I am behind)")
                state.removeNotAcceptedItems()
                return ConsensusHeartbeatResponse(false, currentTerm)
            }

            isUpdatedCommitIndex && transactionBlocker.isAcquired() -> {
                logger.info("Received heartbeat when is blocked so only accepted changes, blocked on ${transactionBlocker.getChangeId()}")
                updateLedger(heartbeat, leaderCommitId, acceptedChangesFromProposed)
                return ConsensusHeartbeatResponse(true, currentTerm, true)
            }

            notAppliedProposedChanges.isNotEmpty() && transactionBlocker.isAcquired() -> {
                logger.info("Received heartbeat but is blocked, so can't accept proposed changes")
                return ConsensusHeartbeatResponse(false, currentTerm, true)
            }

            isUpdatedCommitIndex && areProposedChangesIncompatible -> {
                logger.info("Received heartbeat but changes are incompatible, updated accepted changes")
                updateLedger(heartbeat, leaderCommitId, acceptedChangesFromProposed)
                return ConsensusHeartbeatResponse(true, currentTerm, incompatibleWithHistory = true)
            }

            areProposedChangesIncompatible -> {
                logger.info("Received heartbeat but changes are incompatible")
                return ConsensusHeartbeatResponse(false, currentTerm, incompatibleWithHistory = true)
            }

            notAppliedProposedChanges.isNotEmpty() -> {
                logger.info("Introduce new changes")
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, notAppliedProposedChanges.first().changeId)
            }
        }

        updateLedger(heartbeat, leaderCommitId, notAppliedProposedChanges)

        return ConsensusHeartbeatResponse(true, currentTerm)
    }

    private suspend fun updateLedger(
        heartbeat: ConsensusHeartbeat,
        leaderCommitId: String,
        proposedChanges: List<LedgerItem> = listOf()
    ) {
        val updateResult: LedgerUpdateResult = state.updateLedger(leaderCommitId, proposedChanges)

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
            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = proposedItem.changeId,
                    peerId = peerResolver.currentPeer().peerId,
                    peersetId = peerResolver.currentPeer().peersetId,
                    protocolName = ProtocolName.CONSENSUS,
                    state = "proposed"
                )
            }
            signalPublisher.signal(
                signal = Signal.ConsensusFollowerChangeProposed,
                subject = this,
                peers = listOf(otherConsensusPeers()),
                change = Change.fromHistoryEntry(proposedItem.entry),
                historyEntry = proposedItem.entry
            )
        }

        updateResult.acceptedItems.forEach {
            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = it.changeId,
                    peerId = peerResolver.currentPeer().peerId,
                    peersetId = peerResolver.currentPeer().peersetId,
                    protocolName = ProtocolName.CONSENSUS,
                    state = "accepted"
                )
            }
            changeIdToCompletableFuture[it.changeId]?.complete(ChangeResult(ChangeResult.Status.SUCCESS))
        }

        val message = "Received heartbeat from ${heartbeat.leaderId} with " +
                "${updateResult.proposedItems.size} proposed and " +
                "${updateResult.acceptedItems.size} accepted items"
        logger.debug(message)

        if (updateResult.proposedItems.isNotEmpty() || updateResult.acceptedItems.isNotEmpty()) {
            logger.info(message)
        }

        updateResult.acceptedItems.find { it.changeId == transactionBlocker.getChangeId() }?.let {
            transactionBlocker.tryToReleaseBlockerChange(
                ProtocolName.CONSENSUS,
                it.changeId
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

    override suspend fun getProposedChanges(): List<Change> = state.getLogEntries()
        .reversed()
        .takeWhile { it.entry.getId() != state.lastApplied }
        .mapNotNull { Change.fromHistoryEntry(it.entry) }


    override suspend fun getAcceptedChanges(): List<Change> =
        state.getLogEntries()
            .let { entries ->
                val index = entries.indexOfFirst { it.entry.getId() == state.lastApplied }
                history.toEntryList() + entries.take(index + 1).map { it.entry }
            }.mapNotNull { Change.fromHistoryEntry(it) }

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
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.leaderCommitId)
            }

            response.message.success && response.message.incompatibleWithHistory -> {
                logger.info("Peer $peer's history is incompatible with proposed change but accepted some changes so retry proposing")
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.leaderCommitId)
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
                    peerMessage.leaderCommitId,
                    peerMessage.logEntries
                )
            }

            response.message.term > currentTerm -> {
                logger.info("Based on info from $peer, someone else is currently leader")
                mutex.withLock {
                    stopBeingLeader(response.message.term)
                    votedFor = null
                }
            }


            response.message.term <= currentTerm -> {
                logger.info("Peer ${peerAddress.globalPeerId} is not up to date, decrementing index")
                val oldValues = peerUrlToNextIndex[peerAddress.globalPeerId]
//                Done: Add decrement method for PeerIndices
                peerUrlToNextIndex[peerAddress.globalPeerId] = oldValues
                    ?.decrement(state)
                    ?: PeerIndices()
            }
        }

        checkIfQueuedChanges()
    }

    private suspend fun applyAcceptedChanges() {

        val acceptedItems: List<LedgerItem>

        mutex.withLock {
            val acceptedIds: List<String> = voteContainer.getAcceptedChanges { isMoreThanHalf(it) }
            acceptedItems = state.getLogEntries(acceptedIds)

            acceptedItems.find { it.changeId == transactionBlocker.getChangeId() }?.let {
                logger.info("Applying accepted changes: $acceptedItems")
                logger.debug("In applyAcceptedChanges should tryToReleaseBlocker")
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, it.changeId)
            }

            state.acceptItems(acceptedIds)
            voteContainer.removeChanges(acceptedIds)
        }

        acceptedItems.forEach {
            changeIdToCompletableFuture.putIfAbsent(it.changeId, CompletableFuture())
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

        acceptedItems
            .map { changeIdToCompletableFuture[it.changeId] }
            .forEach { it!!.complete(ChangeResult(ChangeResult.Status.SUCCESS)) }

        if (acceptedItems.isNotEmpty()) scheduleHeartbeatToPeers()
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
        leaderCommitId: String,
        newProposedChanges: List<LedgerItemDto> = listOf()
    ) {
        val globalPeerId = peerAddress.globalPeerId

        val peerIndices = peerUrlToNextIndex.getOrDefault(globalPeerId, PeerIndices())

        if (newProposedChanges.isNotEmpty()) {
            val proposedChanges = newProposedChanges.map { it.toLedgerItem() }

            proposedChanges
                .filter { state.isNotApplied(it.entry.getId()) }
                .forEach { voteContainer.voteForChange(it.entry.getId()) }
            peerUrlToNextIndex[globalPeerId] =
                peerIndices.copy(acknowledgedEntryId = proposedChanges.last().entry.getId())
        }

        if (peerIndices.acceptedEntryId != leaderCommitId) {
            val newPeerIndices = peerUrlToNextIndex.getOrDefault(globalPeerId, PeerIndices())
            peerUrlToNextIndex[globalPeerId] =
                newPeerIndices.copy(acceptedEntryId = leaderCommitId)
        }

        applyAcceptedChanges()
    }

    private suspend fun getMessageForPeer(peerAddress: PeerAddress): ConsensusHeartbeat {
        state.checkCommitIndex()

        val peerIndices = peerUrlToNextIndex.getOrDefault(peerAddress.globalPeerId, PeerIndices()) // TODO
        val newProposedChanges = state.getNewProposedItems(peerIndices.acknowledgedEntryId)
        val lastAppliedChangeId = peerIndices.acceptedEntryId

        return ConsensusHeartbeat(
            peerId,
            currentTerm,
            newProposedChanges.map { it.toDto() },
            lastAppliedChangeId,
            history.getCurrentEntry().getId(),
            state.commitIndex
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

            val updatedChange: Change

            val peersetId = peerResolver.currentPeer().peersetId

            updatedChange = TwoPC.updateParentIdFor2PCCompatibility(change, history, peersetId)
            entry = updatedChange.toHistoryEntry(peersetId)

            try {
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, updatedChange.id)
            } catch (ex: AlreadyLockedException) {
                logger.info("Is already blocked on other transaction ${transactionBlocker.getProtocolName()}")
                result.complete(ChangeResult(ChangeResult.Status.CONFLICT))
                throw ex
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
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, updatedChange.id)
                return
            }

            logger.info("Propose change to ledger: $updatedChange")
            state.proposeEntry(entry, updatedChange.id)
            voteContainer.initializeChange(entry.getId())
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

    private fun releaseBlockerFromPreviousTermChanges() {

        val blockedChange = state.proposedEntries.find { it.changeId == transactionBlocker.getChangeId() }

        if (transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() == ProtocolName.CONSENSUS
            && blockedChange != null
        ) {
            logger.info("Release blocker for changes from previous term")
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, blockedChange.changeId)

            changeIdToCompletableFuture[state.proposedEntries.last().changeId]
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
                result.complete(ChangeResult(ChangeResult.Status.TIMEOUT, "There should always be a leader"))
                throw Exception("There should always be a leader")
            }
        }
        return result
    }

    private fun scheduleHeartbeatToPeers() {
        otherConsensusPeers().forEach {
            launchHeartBeatToPeer(it.globalPeerId, true)
        }
    }

    private fun launchHeartBeatToPeer(peer: GlobalPeerId, sendInstant: Boolean = false): Job =
        with(CoroutineScope(executorService!!)) {
            launch(MDCContext()) {
                if (!sendInstant) delay(heartbeatDelay.toMillis())
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

data class PeerIndices(
    val acceptedEntryId: String = InitialHistoryEntry.getId(),
    val acknowledgedEntryId: String = InitialHistoryEntry.getId()
) {
    fun decrement(ledger: Ledger): PeerIndices = ledger
        .getPreviousEntryId(acceptedEntryId)
        .let { PeerIndices(it, it) }
}

data class VotedFor(val id: Int, val address: String, val elected: Boolean = false)
