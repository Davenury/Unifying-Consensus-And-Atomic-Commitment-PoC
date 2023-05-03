package com.github.davenury.ucac.consensus.paxos

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.txblocker.TransactionAcquisition
import com.github.davenury.common.txblocker.TransactionBlocker
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.consensus.VotedFor
import com.github.davenury.ucac.consensus.raft.ChangeToBePropagatedToLeader
import com.zopa.ktor.opentracing.span
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors

class PaxosProtocolImpl(
    private val peersetId: PeersetId,
    private val history: History,
    private val ctx: ExecutorCoroutineDispatcher,
    private var peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val protocolClient: PigPaxosProtocolClient,
    private val heartbeatTimeout: Duration = Duration.ofSeconds(4),
    private val heartbeatDelay: Duration = Duration.ofMillis(500),
    private val transactionBlocker: TransactionBlocker,
    private val isMetricTest: Boolean
) : PaxosProtocol, SignalSubject {
    //  General consesnus
    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()
    private val globalPeerId = peerResolver.currentPeer()
    private val mutex = Mutex()

    // PigPaxos
    private val executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val entryIdPaxosRound: ConcurrentHashMap<String, PaxosRound> = ConcurrentHashMap()
    private var failureDetector = ProtocolTimerImpl(Duration.ofSeconds(0), heartbeatTimeout, ctx)
    private var votedFor: VotedFor? = null
    private var currentRound = -1
    private val acceptedChannel: Channel<Pair<String, PaxosAccepted?>> = Channel()
    private var changesToBePropagatedToLeader: ConcurrentLinkedDeque<ChangeToBePropagatedToLeader> =
        ConcurrentLinkedDeque()

    private val leaderRequestExecutorService = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private var lastPropagatedEntryId: String = history.getCurrentEntryId()

//  Distinguished proposer, the last one which proposed value, asks him about results
//  Add synchronization phase from Zab after election of the proposer/leader

    companion object {
        private val logger = LoggerFactory.getLogger("pig-paxos")
    }

    override fun getPeerName(): String = globalPeerId.toString()

    override suspend fun begin() {
        failureDetector.startCounting {
            if (votedFor?.elected != true) becomeLeader("No leader was elected")
        }
    }

    override suspend fun handlePropose(message: PaxosPropose): PaxosPromise = span("PigPaxos.handlePropose") {
        mutex.withLock {
            logger.info("Handle PaxosPropose: ${message.paxosRound} ${message.peerId} ${message.lastEntryId} ")

            val committedEntries =
                if (history.containsEntry(message.lastEntryId)) history.getAllEntriesUntilHistoryEntryId(message.lastEntryId)
                else listOf()
            val proposedEntries = entryIdPaxosRound.map { it.value.entry }

            if (currentRound > message.paxosRound || (currentRound == message.paxosRound && votedFor?.id != message.peerId)) {
                return@withLock PaxosPromise(
                    false,
                    currentRound,
                    votedFor?.id,
                    listOf(),
                    listOf(),
                    history.getCurrentEntryId()
                )
            }

            failureDetector.cancelCounting()
            failureDetector = getHeartbeatTimer()
            updateVotedFor(message.paxosRound, message.peerId)

            return PaxosPromise(
                true,
                message.paxosRound,
                message.peerId,
                committedEntries.serialize(),
                proposedEntries.serialize(),
                history.getCurrentEntryId()
            )
        }
    }

    override suspend fun handleAccept(message: PaxosAccept): PaxosAccepted = span("PigPaxos.handleAccept") {
        mutex.withLock {
            val entry = HistoryEntry.deserialize(message.entry)
            val change = Change.fromHistoryEntry(entry)!!
            val changeId = change.id

            logger.info("Handle PaxosAccept: ${message.paxosRound} ${message.proposer} ${entry.getId()}")

            if (isMessageFromNotLeader(message.paxosRound, message.proposer)) {
                logger.info("Reject PaxosAccept, because message is not from leader, current leader ${votedFor?.id}")
                return@withLock PaxosAccepted(
                    false,
                    currentRound,
                    votedFor?.id,
                    currentEntryId = history.getCurrentEntryId()
                )
            }

            val transactionAcquisition = TransactionAcquisition(ProtocolName.CONSENSUS, changeId)
            val isTransactionBlockerAcquired = transactionBlocker.tryAcquireReentrant(transactionAcquisition)

            if (!isTransactionBlockerAcquired) {
                return@withLock PaxosAccepted(
                    false,
                    currentRound,
                    votedFor?.id,
                    isTransactionBlocked = true,
                    currentEntryId = history.getCurrentEntryId()
                )
            }


            if (!history.isEntryCompatible(entry)) {
                logger.info("Reject PaxosAccept, because entry is not compatible, currentEntryId: ${history.getCurrentEntryId()}, entryParentId: ${entry.getParentId()}")
                return@withLock PaxosAccepted(
                    false,
                    currentRound,
                    votedFor?.id,
                    currentEntryId = history.getCurrentEntryId()
                )
            }

            updateVotedFor(message.paxosRound, message.proposer)

            signalPublisher.signal(
                Signal.PigPaxosReceivedAccept,
                this@PaxosProtocolImpl,
                mapOf(peersetId to otherConsensusPeers()),
                change = change
            )

            if (!history.containsEntry(entry.getId())) {
                entryIdPaxosRound[entry.getId()] = PaxosRound(message.paxosRound, entry, message.proposer)
            } else {
                transactionBlocker.tryRelease(transactionAcquisition)
            }

            failureDetector.cancelCounting()
            failureDetector.startCounting {
                logger.info("Try to finish: $message")
                changeIdToCompletableFuture.putIfAbsent(changeId, CompletableFuture())
                changesToBePropagatedToLeader.add(
                    ChangeToBePropagatedToLeader(
                        change,
                        changeIdToCompletableFuture[changeId]!!
                    )
                )
                becomeLeader("Leader didn't finish entry ${entry.getId()}")
            }

            return@withLock PaxosAccepted(
                true,
                currentRound,
                votedFor?.id,
                currentEntryId = history.getCurrentEntryId()
            )
        }
    }

    override suspend fun handleCommit(message: PaxosCommit): Unit = span("PigPaxos.handleCommit") {
        mutex.withLock {
            val entry = HistoryEntry.deserialize(message.entry)
            val change = Change.fromHistoryEntry(entry)!!
            signalPublisher.signal(
                Signal.PigPaxosReceivedCommit,
                this@PaxosProtocolImpl,
                mapOf(peersetId to otherConsensusPeers()),
                change = change
            )
            logger.info("Handle PaxosCommit: ${message.paxosRound} ${message.proposer} ${message.paxosResult} ${entry.getId()}")
            failureDetector.cancelCounting()

            updateVotedFor(message.paxosRound, message.proposer)

            commitChange(message.paxosResult, change)
        }
    }

    override suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult> =
        proposeChangeAsync(change)

    override suspend fun <A> broadcast(message: A) {
        TODO("Not yet implemented")
    }

    override suspend fun <A> send(message: A, toNode: PeerAddress) {
        TODO("Not yet implemented")
    }

    override fun getLeaderId(): PeerId? = if (votedFor?.elected == true) votedFor?.id else null


    override fun stop() {
        executorService.close()
    }

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> =
        span("PigPaxos.proposeChangeAsync") {

            if(changeIdToCompletableFuture.contains(change.id)) return changeIdToCompletableFuture[change.id]!!

            val result = changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
                ?: changeIdToCompletableFuture[change.id]!!

            if (amIALeader() && lastPropagatedEntryId == history.getCurrentEntryId()) {
                proposeChangeToLedger(result, change)
            } else {
                logger.info("We are not a leader or some change maybe needed to propagate")
                changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, result))
                becomeLeader("Try to process change")
            }

            return result
        }

    override suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change) =
        span("PigPaxos.proposeChangeToLedger") {
            val entry = change.toHistoryEntry(peersetId)
            if (entryIdPaxosRound.contains(entry.getId())) {
                logger.info("Already proposed that change: $change")
                return
            }

            if (transactionBlocker.isAcquired() && transactionBlocker.getChangeId() != change.id) {
                logger.info("Queued change, because: transaction is blocked")
                result.complete(ChangeResult(ChangeResult.Status.TIMEOUT))
                return
            }

            try {
                transactionBlocker.acquireReentrant(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
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
                        change.toHistoryEntry(peersetId).getParentId()
                    }"
                )
                result.complete(ChangeResult(ChangeResult.Status.CONFLICT))
                transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                return
            }
            logger.info("Propose change to ledger: $change")
            acceptPhase(entry)
        }


    private fun updateVotedFor(paxosRound: Int, proposer: PeerId) {
        if (paxosRound > currentRound) {
            votedFor = VotedFor(proposer, true)
            currentRound = paxosRound
            cleanOldRoundState()
            sendOldChanges()
        }
    }

    override fun getState(): History = history

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override fun otherConsensusPeers(): List<PeerAddress> =
        peerResolver.getPeersFromPeerset(peersetId).filter { it.peerId != globalPeerId }

    override suspend fun getProposedChanges(): List<Change> = span("PigPaxos.getProposedChanges") {
        mutex.withLock {
            entryIdPaxosRound
                .values
                .mapNotNull { Change.fromHistoryEntry(it.entry) }
        }
    }

    override suspend fun getAcceptedChanges(): List<Change> = span("PigPaxos.getAcceptedChanges") {
        mutex.withLock {
            history
                .toEntryList(true)
                .mapNotNull { Change.fromHistoryEntry(it) }
        }
    }

    private suspend fun sendRequestToLeader(cf: CompletableFuture<ChangeResult>, change: Change): Unit =
        span("PigPaxos.sendRequestToLeader") {
            with(CoroutineScope(leaderRequestExecutorService)) {
                launch(MDCContext()) {
                    if (amIALeader()) return@launch proposeChangeToLedger(cf, change)

                    var result: ChangeResult? = null
//              It won't be infinite loop because if leader exists we will finally send message to him and if not we will try to become one
                    var retries = 0
                    while (result == null && retries < 3) {
                        val address: PeerAddress
                        if (amIALeader()) {
                            return@launch proposeChangeToLedger(cf, change)
                        } else {
                            address = peerResolver.resolve(votedFor!!.id)
                        }

                        result = try {
                            protocolClient.sendRequestApplyChange(address, change)
                        } catch (e: Exception) {
                            logger.error("Request to leader ($address, ${votedFor?.id}) failed", e.message)
                            null
                        }
                        retries++
                        if (result == null) delay(heartbeatDelay.toMillis())
                    }

                    if (result == null) {
                        logger.info("Sending request to leader failed, try to become a leader myself")
                        changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, cf))
                        return@launch becomeLeader("Leader doesn't respond to my request")
                    }

                    if (result.status != ChangeResult.Status.SUCCESS) {
                        entryIdPaxosRound.remove(change.toHistoryEntry(peersetId).getId())
                        cf.complete(result)
                    }


                }
            }
        }

    private suspend fun becomeLeader(reason: String, round: Int = currentRound + 1): Unit =
        span("PigPaxos.becomeLeader") {

            mutex.withLock {
                if (round <= currentRound) {
                    logger.info("Don't try to become a leader, because someone tried in meantime, propagate changes")
                    sendOldChanges()
                    return@span
                }

                currentRound = round
                votedFor = VotedFor(globalPeerId)
            }
            logger.info("Try to become a leader in round: $round, because $reason")
            signalPublisher.signal(
                Signal.PigPaxosTryToBecomeLeader,
                this@PaxosProtocolImpl,
                mapOf(peersetId to otherConsensusPeers())
            )

            val responses: List<PaxosPromise> = protocolClient
                .sendProposes(otherConsensusPeers(), PaxosPropose(globalPeerId, round, history.getCurrentEntryId()))
                .mapNotNull { it.message }

            val newRound = responses.maxOfOrNull { it.currentRound } ?: currentRound

            val peerTryingToBecomeLeaderInSameRound =
                responses.find { !it.promised && it.currentRound == round && it.currentLeaderId != globalPeerId }

            val newerLeader = responses.find { !it.promised && it.currentLeaderId != null && it.currentRound > round }
            val votes = responses.filter { it.promised }.count { it.promised }

            mutex.withLock {
                when {
                    currentRound > round && votedFor?.elected == true -> {
                        logger.info("Other peer ${votedFor?.id} become a leader meanwhile myself tried")
                    }

                    newerLeader?.currentLeaderId != null && newerLeader.currentRound > round -> {
                        logger.info("Peer ${newerLeader.currentLeaderId} can be a leader in round: ${newerLeader.currentRound}")
                        votedFor = VotedFor(newerLeader.currentLeaderId, true)
                        currentRound = maxOf(newerLeader.currentRound, newRound)
                    }

                    newerLeader != null && newerLeader.currentLeaderId == null -> {
                        logger.info("This is not newest round, retry becoming leader in some time")
                        currentRound = maxOf(newerLeader.currentRound, newRound)
                        failureDetector.startCounting {
                            becomeLeader("Lea")
                        }
                        return
                    }

                    peerTryingToBecomeLeaderInSameRound != null -> {
                        logger.info("Peer ${peerTryingToBecomeLeaderInSameRound.currentLeaderId} tries to become a leader in the same round, retry after some time")
                        currentRound = maxOf(currentRound, newRound)
                        failureDetector.startCounting {
                            becomeLeader("Two peers tried to become a leader in the same time")
                        }
                        return
                    }

                    isMoreThanHalf(votes) -> {
                        logger.info("I have been selected as a leader in round $round")
                        Metrics.bumpLeaderElection(peerResolver.currentPeer(), peersetId)
                        votedFor = VotedFor(globalPeerId, true)
                        signalPublisher.signal(
                            Signal.PigPaxosLeaderElected,
                            this@PaxosProtocolImpl,
                            mapOf(peersetId to otherConsensusPeers())
                        )

                        val committedEntries = responses.flatMap { it.committedEntries.deserialize() }

                        val newCommittedEntries =
                            responses.flatMap { history.getAllEntriesUntilHistoryEntryId(it.currentEntryId) }

                        val proposedEntries = responses.flatMap { it.notFinishedEntries.deserialize() }

                        val oldChangesToBePropagated =
                            changesToBePropagatedToLeader.toList().map { it.change.toHistoryEntry(peersetId) }

                        changesToBePropagatedToLeader = ConcurrentLinkedDeque()

                        (committedEntries + newCommittedEntries + proposedEntries + oldChangesToBePropagated).distinct()
                            .forEach {
                                val change = Change.fromHistoryEntry(it)!!
                                changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
                                changesToBePropagatedToLeader.add(
                                    ChangeToBePropagatedToLeader(
                                        change,
                                        changeIdToCompletableFuture[change.id]!!
                                    )
                                )
                            }


                    }

                    else -> {
                        logger.info("I don't gather quorum votes, retry after some time")
                        failureDetector.startCounting {
                            becomeLeader("Leader wasn't elected, retry after some time")
                        }
                        return
                    }
                }
            }

            tryPropagatingChangesToLeader()
        }


    private suspend fun acceptPhase(entry: HistoryEntry) {
        var result: PaxosResult? = null
        val change = Change.fromHistoryEntry(entry)!!
        entryIdPaxosRound[entry.getId()] = PaxosRound(currentRound, entry, votedFor?.id!!)
        while (result == null) {
            result = sendAccepts(entry)
            if (!amIALeader()) {
                logger.info("I am not a leader, so stop sending accepts")
                changesToBePropagatedToLeader.add(
                    ChangeToBePropagatedToLeader(
                        change,
                        changeIdToCompletableFuture[change.id]!!
                    )
                )

                tryPropagatingChangesToLeader()
                return
            } else {
                logger.info("Result of sending accepts is $result")
            }
            if (result == null) {
                logger.info("Delay sending accepts for ${heartbeatDelay.toMillis()}")
                delay(heartbeatDelay.toMillis())
            }
        }

        logger.info("Accepts result is: $result")

        signalPublisher.signal(Signal.PigPaxosAfterAcceptChange, this, mapOf(peersetId to otherConsensusPeers()))
        mutex.withLock {
            commitChange(result, change)
            if (result == PaxosResult.COMMIT) lastPropagatedEntryId = entry.getId()
        }

        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    do {
                        val peerAddress: PeerAddress = otherConsensusPeers()[it]
                        val response: ConsensusResponse<String?> = protocolClient.sendCommit(
                            peerAddress,
                            PaxosCommit(result, entry.serialize(), currentRound, votedFor?.id!!)
                        )
                        if (response.message == null) delay(heartbeatDelay.toMillis())
                    } while (response.message == null && amIALeader())
                }
            }
        }
    }

    private suspend fun commitChange(result: PaxosResult, change: Change) {
        val entry = change.toHistoryEntry(peersetId)

        when {
            changeIdToCompletableFuture[change.id]?.isDone == true -> {
                entryIdPaxosRound.remove(entry.getId())
            }

            result == PaxosResult.COMMIT && history.isEntryCompatible(entry) -> {
                logger.info("Commit entry $entry")
                if (!history.containsEntry(entry.getId())) {
                    history.addEntry(entry)
                    entryIdPaxosRound.remove(entry.getId())
                    transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                    changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.SUCCESS))
                    signalPublisher.signal(
                        Signal.PigPaxosChangeCommitted,
                        this,
                        mapOf(peersetId to otherConsensusPeers()),
                        change = change
                    )
                }else {
                    entryIdPaxosRound.remove(entry.getId())
                    transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                }
            }

            result == PaxosResult.COMMIT -> {

//              TODO: Send propose in which you learnt about all committed changes since your currentEntryId

//              Send PaxosPropose with PaxosRound on -1 to get consensus state about accepted entries
                val responses = protocolClient.sendProposes(
                    otherConsensusPeers(),
                    PaxosPropose(globalPeerId, -1, history.getCurrentEntryId())
                ).mapNotNull { it.message }

                val maxEntryResponse =
                    responses.maxWithOrNull { a, b -> a.committedEntries.size - b.committedEntries.size }

                maxEntryResponse?.committedEntries
                    ?.map { HistoryEntry.deserialize(it) }
                    ?.forEach {
                        if (history.isEntryCompatible(it)) history.addEntry(it)
                    }


                if (history.isEntryCompatible(entry)) history.addEntry(entry)
                else {
                    logger.error("Inconsistent state in peerset, parent of entry to be commited isn't committed on quorum")
                    throw RuntimeException("Inconsistent state it should be fixed")
                }

            }

            result == PaxosResult.ABORT -> {
                logger.info("Abort entry $entry")
                changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.CONFLICT))
                signalPublisher.signal(
                    Signal.PigPaxosChangeAborted,
                    this,
                    mapOf(peersetId to otherConsensusPeers()),
                    change = change
                )
                entryIdPaxosRound.remove(entry.getId())
                transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
            }
        }
    }

    private suspend fun sendAccepts(entry: HistoryEntry): PaxosResult? {
        val changeId = Change.fromHistoryEntry(entry)!!.id
        val jobs = scheduleAccepts(changeId) {
            protocolClient.sendAccept(
                it,
                PaxosAccept(entry.serialize(), currentRound, votedFor?.id!!, history.getCurrentEntryId())
            )
        }

        val responses = gatherAccepts(currentRound, changeId).filterNotNull()
        logger.info("Responses sendAccepts: $responses")

        val (accepted, rejected) = responses.partition { it.accepted }

        logger.info("Accepts: ${accepted.size} Rejected: ${rejected.size}")

        return when {
            isMoreThanHalf(accepted.size) -> PaxosResult.COMMIT

            isMoreThanHalf(rejected.size) -> PaxosResult.ABORT

            else -> null
        }
    }

    //  Send tou many nulls
    private suspend fun scheduleAccepts(
        changeId: String,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<PaxosAccepted?>
    ) =
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    val peerAddress: PeerAddress = otherConsensusPeers()[it]
                    val response: ConsensusResponse<PaxosAccepted?> = sendMessage(peerAddress)

                    when {
                        response.message == null -> acceptedChannel.send(Pair(changeId, null))
                        response.message.isTransactionBlocked -> {
                            logger.info("Peer has transaction blocked")
                            acceptedChannel.send(Pair(changeId, null))
                        }

                        history.containsEntry(response.message.currentEntryId) && response.message.currentEntryId != history.getCurrentEntryId() -> {
                            logger.info("Peer has outdated history")
                            history.getAllEntriesUntilHistoryEntryId(response.message.currentEntryId).forEach {
                                protocolClient.sendCommit(
                                    peerAddress,
                                    PaxosCommit(PaxosResult.COMMIT, it.serialize(), currentRound, globalPeerId)
                                )
                            }
                            acceptedChannel.send(Pair(changeId, null))
                        }

                        else -> acceptedChannel.send(Pair(changeId, response.message))
                    }
                }
            }
        }

    private suspend fun gatherAccepts(round: Int, changeId: String): List<PaxosAccepted?> {
        val responses: MutableList<PaxosAccepted?> = mutableListOf()

        val peers = otherConsensusPeers().size

        while (responses.size < peers) {
            val tuple = acceptedChannel.receive()

            if (changeId != tuple.first) continue

            val response = tuple.second
            responses.add(response)

            val (accepted, rejected) = responses.filterNotNull().partition { it.result }

            logger.info("Received response: $response, accepted: ${accepted.size}, rejected: ${rejected.size}, total: ${responses.size}")

            when {
                response == null -> {}

                isNotValidLeader(response, round) && round >= 0 -> {
                    mutex.withLock {
                        votedFor = VotedFor(response.currentLeaderId!!, true)
                        currentRound = response.currentRound
                    }

                    logger.info("I am not a valid leader anymore, current leader: ${response.currentLeaderId}, round: ${response.currentRound}")
                    return responses
                }

                isMoreThanHalf(accepted.size) || isMoreThanHalf(rejected.size) -> {
                    return responses
                }
            }
        }

        return responses
    }

    private fun cleanOldRoundState() {
        val oldEntries = entryIdPaxosRound.values.filter { it.round < currentRound }
        oldEntries.forEach {
            val change = Change.fromHistoryEntry(it.entry)!!
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            changesToBePropagatedToLeader.add(
                ChangeToBePropagatedToLeader(
                    change,
                    changeIdToCompletableFuture[change.id]!!
                )
            )
            entryIdPaxosRound.remove(it.entry.getId())
        }
        val changeId = transactionBlocker.getChangeId()
        if (changeId != null) transactionBlocker.tryRelease(
            TransactionAcquisition(
                ProtocolName.CONSENSUS,
                changeId
            )
        )
    }

    private fun sendOldChanges() {
        with(CoroutineScope(leaderRequestExecutorService)) {
            launch(MDCContext()) {
                tryPropagatingChangesToLeader()
            }
        }
    }


    private fun isNotValidLeader(message: PaxosResponse, round: Int = currentRound): Boolean =
        message.currentRound > round || (message.currentRound == round && message.currentLeaderId != globalPeerId)

    private fun isMessageFromNotLeader(round: Int, leaderId: PeerId) =
        currentRound > round || (currentRound == round && leaderId != votedFor?.id)

    private fun amIALeader() = votedFor?.elected == true && votedFor?.id == globalPeerId

    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)


    private suspend fun tryPropagatingChangesToLeader() {
        // TODO mutex?
        val votedFor = this.votedFor
        if (votedFor == null || !votedFor.elected) return
        if (changesToBePropagatedToLeader.size > 0) logger.info("Try to propagate changes")
        while (true) {
            val changeToBePropagated = changesToBePropagatedToLeader.poll() ?: break
            if (amIALeader()) {
                logger.info("Processing a queued change as a leader: ${changeToBePropagated.change}")
                proposeChangeToLedger(changeToBePropagated.cf, changeToBePropagated.change)
            } else {
                logger.info("Propagating a change to the leader (${votedFor.id}): ${changeToBePropagated.change}")
                sendRequestToLeader(changeToBePropagated.cf, changeToBePropagated.change)
            }
        }
    }

}

data class PaxosRound(val round: Int, val entry: HistoryEntry, val proposerId: PeerId)