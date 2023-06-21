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
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.consensus.SynchronizationMeasurement
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
import java.time.Instant
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
    private val isMetricTest: Boolean,
    private val subscribers: Subscribers?,
    private val maxChangesPerMessage: Int,
) : PaxosProtocol, SignalSubject {
    //  General consesnus
    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()
    private val globalPeerId = peerResolver.currentPeer()
    private val mutex = Mutex()

    // PigPaxos
    private val executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val entryIdPaxosRound: ConcurrentHashMap<String, PaxosRound> = ConcurrentHashMap()
    private var leaderFailureDetector = ProtocolTimerImpl(Duration.ofSeconds(0), heartbeatTimeout, ctx)
    private var votedFor: VotedFor? = null
    private var currentRound = -1
    private var queuedChange: ConcurrentLinkedDeque<ChangeToBePropagatedToLeader> =
        ConcurrentLinkedDeque()

    private val leaderRequestExecutorService = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private var lastPropagatedEntryId: String = history.getCurrentEntryId()
    private val peerIdToEntryId: ConcurrentHashMap<PeerId, String> = ConcurrentHashMap()

    private val synchronizationMeasurement =
        SynchronizationMeasurement(history, protocolClient, this, globalPeerId)


    companion object {
        private val logger = LoggerFactory.getLogger("pig-paxos")
    }

    override fun getPeerName(): String = globalPeerId.toString()

    override suspend fun begin() {
        synchronizationMeasurement.begin(ctx)
        leaderFailureDetector.startCounting {
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

            leaderFailureDetector.cancelCounting()
            leaderFailureDetector = getHeartbeatTimer()
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

            signalPublisher.signal(
                Signal.PigPaxosBeginHandleMessages,
                this@PaxosProtocolImpl,
                mapOf(peersetId to otherConsensusPeers()),
                change = change
            )

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

            if (isTransactionFinished(entry.getId(), changeId)) return@span PaxosAccepted(
                true,
                currentRound,
                votedFor?.id,
                currentEntryId = history.getCurrentEntryId()
            )

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

            entryIdPaxosRound[entry.getId()] = PaxosRound(message.paxosRound, entry, message.proposer)
            resetFailureDetector(entry, change)

            return@withLock PaxosAccepted(
                true,
                currentRound,
                votedFor?.id,
                currentEntryId = history.getCurrentEntryId()
            )
        }
    }

    override suspend fun handleCommit(message: PaxosCommit): PaxosCommitResponse = span("PigPaxos.handleCommit") {

        val entry = HistoryEntry.deserialize(message.entry)
        val change = Change.fromHistoryEntry(entry)!!
        if (isTransactionFinished(entry.getId(), change.id)) {
            return@span PaxosCommitResponse(history.getCurrentEntryId(), true)
        }

        signalPublisher.signal(
            Signal.PigPaxosBeginHandleMessages,
            this@PaxosProtocolImpl,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )
        signalPublisher.signal(
            Signal.PigPaxosReceivedCommit,
            this@PaxosProtocolImpl,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )
        logger.info("Handle PaxosCommit: ${message.paxosRound} ${message.proposer} ${message.paxosResult} ${entry.getId()}")
        mutex.withLock {
            leaderFailureDetector.cancelCounting()
            updateVotedFor(message.paxosRound, message.proposer)
        }
        commitChange(message.paxosResult, change)
        return@span PaxosCommitResponse(history.getCurrentEntryId(), isTransactionFinished(entry.getId(), change.id))
    }

    override suspend fun handleBatchCommit(message: PaxosBatchCommit) {

        val entries = message.entries.map { HistoryEntry.deserialize(it) }
        val changes = entries.map { Change.fromHistoryEntry(it)!! }
        changes.forEach {
            signalPublisher.signal(
                Signal.PigPaxosBeginHandleMessages,
                this@PaxosProtocolImpl,
                mapOf(peersetId to otherConsensusPeers()),
                change = it
            )
        }

        logger.info("Handle PaxosBatchCommit: ${message.paxosResult}, entries: ${entries.size}")
        mutex.withLock {
            updateVotedFor(message.paxosRound, message.proposer)
        }

        changes.forEach {
            commitChange(message.paxosResult, it)
        }
    }

    override suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult> {

        val entry = change.toHistoryEntry(peersetId)

        mutex.withLock {
            if (!history.containsEntry(entry.getParentId()!!)) throw PaxosLeaderBecameOutdatedException(change.id)
        }

        return proposeChangeAsync(change)
    }

    override fun getLeaderId(): PeerId? = if (votedFor?.elected == true) votedFor?.id else null


    override fun stop() {
        executorService.close()
    }

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {

        if (changeIdToCompletableFuture.contains(change.id)) return changeIdToCompletableFuture[change.id]!!

        val result = changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            ?: changeIdToCompletableFuture[change.id]!!


        queuedChange.add(ChangeToBePropagatedToLeader(change, result))
        CoroutineScope(leaderRequestExecutorService).launch {
            tryPropagatingChangesToLeader()
        }

//        if (amILeader() && lastPropagatedEntryId == history.getCurrentEntryId()) {
//            proposeChangeToLedger(result, change)
//        } else {
//            logger.info("We are not a leader or some change maybe needed to propagate")
//            queuedChange.add(ChangeToBePropagatedToLeader(change, result))
//            becomeLeader("Try to process change")
//        }

        return result
    }

    override suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change) {
        val entry = change.toHistoryEntry(peersetId)
        if (entryIdPaxosRound.contains(entry.getId())) {
            logger.info("Already proposed that change: $change")
            return
        }

        val transactionAcquisition = TransactionAcquisition(ProtocolName.CONSENSUS, change.id)

        if (!transactionBlocker.tryAcquireReentrant(transactionAcquisition)) {
            logger.info("Transaction is blocked on protocol ${transactionBlocker.getProtocolName()}, changeId: ${transactionBlocker.getChangeId()}, add transaction to queue, changeId: ${change.id}")
            if (transactionBlocker.getProtocolName() == ProtocolName.CONSENSUS) {
                val changeId = transactionBlocker.getChangeId()!!
                val entry = entryIdPaxosRound
                    .map { it.value.entry }
                    .find { Change.fromHistoryEntry(it)?.id == changeId }

                if (entry != null) {
                    ChangeToBePropagatedToLeader(
                        entry.let { Change.fromHistoryEntry(it)!! },
                        changeIdToCompletableFuture[changeId]!!
                    ).let { queuedChange.add(it) }
                }


                if (leaderFailureDetector.isTaskFinished()) leaderFailureDetector.startCounting {
                    logger.info("Recovery from unfinished entry")
                    if (entry != null) acceptPhase(entry)
                    else {
                        logger.error("Inconsistent state, release transaction blocker for change $changeId")
                        transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, changeId))
                        tryPropagatingChangesToLeader()
                    }
                } else {
                    logger.info("Leader failure detector task is not finished for changeId ${transactionBlocker.getChangeId()}")
                }
            }

            queuedChange.add(ChangeToBePropagatedToLeader(change, result))
            return
        }

        if (!history.isEntryCompatible(entry)) {
            logger.info(
                "Proposed change is incompatible. \n CurrentChange: ${
                    history.getCurrentEntry().getId()
                } \n Change.parentId: ${
                    change.toHistoryEntry(peersetId).getParentId()
                }"
            )
            result.complete(ChangeResult(ChangeResult.Status.CONFLICT, currentEntryId = history.getCurrentEntryId()))
            transactionBlocker.tryRelease(transactionAcquisition)
            return
        }
        logger.info("Propose change to ledger: $change")
        acceptPhase(entry)
    }


    private fun updateVotedFor(paxosRound: Int, proposer: PeerId) {
        if (paxosRound > currentRound) {
            logger.info("Update voted for, current leader: ${proposer.peerId} in round: $paxosRound")
            votedFor = VotedFor(proposer, true)
            currentRound = paxosRound
            cleanOldRoundState()
            sendOldChanges()
        }
    }

    override fun getState(): History = history
    override suspend fun isSynchronized(): Boolean = synchronizationMeasurement.isSynchronized()

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

    private suspend fun sendRequestToLeader(cf: CompletableFuture<ChangeResult>, change: Change): Unit {
        with(CoroutineScope(leaderRequestExecutorService)) {
            launch(MDCContext()) {
                if (amILeader()) return@launch proposeChangeToLedger(cf, change)

                var result: ChangeResult? = null
//              It won't be infinite loop because if leader exists we will finally send message to him and if not we will try to become one
                var retries = 0
                while (result == null && retries < 3) {
                    val address: PeerAddress
                    if (amILeader()) {
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
                    queuedChange.add(ChangeToBePropagatedToLeader(change, cf))
                    return@launch becomeLeader("Leader doesn't respond to my request")
                }

                if (result.status != ChangeResult.Status.SUCCESS) {
                    entryIdPaxosRound.remove(change.toHistoryEntry(peersetId).getId())
                    cf.complete(result)
                }


            }
        }
    }

    private suspend fun becomeLeader(reason: String, round: Int = currentRound + 1): Unit {
        transactionBlocker.getChangeId()
            ?.let { TransactionAcquisition(ProtocolName.CONSENSUS, it) }
            ?.let { transactionBlocker.tryRelease(it) }

        val isTheNewestRound: Boolean
        mutex.withLock {
            isTheNewestRound = round <= currentRound
        }

        if (isTheNewestRound) {
            logger.info("Don't try to become a leader, because someone tried in meantime, propagate changes")
            sendOldChanges()
            return
        }

        mutex.withLock {
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
                    leaderFailureDetector.startCounting {
                        becomeLeader("Retry becoming leader")
                    }
                    return
                }

                peerTryingToBecomeLeaderInSameRound != null -> {
                    logger.info("Peer ${peerTryingToBecomeLeaderInSameRound.currentLeaderId} tries to become a leader in the same round, retry after some time")
                    currentRound = maxOf(currentRound, newRound)
                    leaderFailureDetector.startCounting {
                        becomeLeader("Two peers tried to become a leader in the same time")
                    }
                    return
                }

                isMoreThanHalf(votes) -> {
                    logger.info("I have been selected as a leader in round $round")
                    val peerId = peerResolver.currentPeer()
                    Metrics.bumpLeaderElection(peerId, peersetId)
                    subscribers?.notifyAboutConsensusLeaderChange(peerId, peersetId)
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
                        queuedChange.toList().map { it.change.toHistoryEntry(peersetId) }

                    queuedChange = ConcurrentLinkedDeque()

                    (committedEntries + newCommittedEntries + proposedEntries + oldChangesToBePropagated)
                        .distinct()
                        .forEach {
                            val change = Change.fromHistoryEntry(it)!!
                            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
                            queuedChange.add(
                                ChangeToBePropagatedToLeader(
                                    change,
                                    changeIdToCompletableFuture[change.id]!!
                                )
                            )
                        }


                }

                else -> {
                    logger.info("I don't gather quorum votes, retry after some time")
                    leaderFailureDetector.startCounting {
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
            if (!amILeader()) {
//              TODO: verify that leader is elected, bo jest możliwość rc że oboje peerów myśli że drugi jest liderem
                logger.info("I am not a leader, so stop sending accepts")

                queuedChange.add(
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
            if (result == PaxosResult.COMMIT) lastPropagatedEntryId = entry.getId()
            otherConsensusPeers().forEach {
                peerIdToEntryId[it.peerId] = entry.getId()
            }
        }
        commitChange(result, change)

        logger.info("Send commit message to other peers for entry: ${entry.getId()}")
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    do {
                        val peerAddress: PeerAddress = otherConsensusPeers()[it]
                        logger.info("Send PaxosCommit to ${peerAddress.peerId} for entry: ${entry.getId()}")
                        val response: ConsensusResponse<PaxosCommitResponse?> = protocolClient.sendCommit(
                            peerAddress,
                            PaxosCommit(result, entry.serialize(), currentRound, globalPeerId)
                        )


                        val updatedResponse: ConsensusResponse<PaxosCommitResponse?> = when {
                            response.message == null -> {
                                logger.info("PaxosCommit response from peer ${peerAddress.peerId} response is null")
                                response
                            }

                            !response.message.isFinished -> {
                                logger.info("PaxosCommit response from peer ${peerAddress.peerId} response is entry")
                                sendBatchCommit(response.message.entryId, peerAddress)
                                response.copy(message = null)
                            }

                            else -> {
                                response
                            }

                        }

                        if (updatedResponse.message == null) delay(heartbeatDelay.toMillis())
                    } while (updatedResponse.message == null && amILeader() && peerIdToEntryId[peerAddress.peerId] == entry.getId())
                }
            }
        }

        checkIfQueuedChanges()
    }

    private suspend fun checkIfQueuedChanges() {
        if (queuedChange.isEmpty()) return
        val changeToBePropagatedToLeader = queuedChange.poll()
        proposeChangeToLedger(changeToBePropagatedToLeader.cf, changeToBePropagatedToLeader.change)
    }

    private suspend fun commitChange(result: PaxosResult, change: Change) {
        val entry = change.toHistoryEntry(peersetId)

        mutex.withLock {
            leaderFailureDetector.cancelCounting()
        }
        when {
            changeIdToCompletableFuture[change.id]?.isDone == true -> mutex.withLock {
                logger.info("Entry is already finished, clean after this entry: ${entry.getId()}")
                entryIdPaxosRound.remove(entry.getId())
            }

            result == PaxosResult.COMMIT && history.isEntryCompatible(entry) -> mutex.withLock {
                logger.info("Commit entry ${entry.getId()}")
                if (!history.containsEntry(entry.getId())) {
                    history.addEntry(entry)
                    synchronizationMeasurement.entryIdCommitted(entry.getId(), Instant.now())
                    entryIdPaxosRound.remove(entry.getId())
                    transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                    changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.SUCCESS))
                    signalPublisher.signal(
                        Signal.PigPaxosChangeCommitted,
                        this,
                        mapOf(peersetId to otherConsensusPeers()),
                        change = change
                    )
                } else {
                    entryIdPaxosRound.remove(entry.getId())
                    transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                }

                checkAfterCommit()
            }

            result == PaxosResult.COMMIT -> {
                logger.info("Entry is incompatible even though we should commit it, try recover from it")

//              TODO: Send propose in which you learnt about all committed changes since your currentEntryId
//              Send PaxosPropose with PaxosRound on -1 to get consensus state about accepted entries
                val responses = protocolClient.sendProposes(
                    otherConsensusPeers(),
                    PaxosPropose(globalPeerId, -1, history.getCurrentEntryId())
                ).mapNotNull { it.message }

                val responseEntries = responses
                    .flatMap { it.committedEntries }
                    .distinct()
                    .map { HistoryEntry.deserialize(it) }

                mutex.withLock {
                    responseEntries.forEach {
                        if (history.isEntryCompatible(it)) {
                            history.addEntry(it)
                            synchronizationMeasurement.entryIdCommitted(it.getId(), Instant.now())
                        }
                    }


                    if (history.isEntryCompatible(entry)) {
                        history.addEntry(entry)
                        synchronizationMeasurement.entryIdCommitted(entry.getId(), Instant.now())
                        checkAfterCommit()
                    } else {
                        logger.error("Missing some changes try to finish entry with id ${entry.getId()} after sometime")
                        resetFailureDetector(entry, change)
                        return
                    }
                }
            }

            result == PaxosResult.ABORT -> mutex.withLock {
                logger.info("Abort entry $entry")
                changeIdToCompletableFuture[change.id]?.complete(
                    ChangeResult(
                        ChangeResult.Status.CONFLICT,
                        currentEntryId = history.getCurrentEntryId()
                    )
                )
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

    private fun checkAfterCommit() {
        entryIdPaxosRound.values.forEach {
            if (history.getCurrentEntryId() != it.entry.getParentId()) {
                entryIdPaxosRound.remove(it.entry.getId())
                val change = Change.fromHistoryEntry(it.entry)!!
                changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.CONFLICT))
            }
        }
    }

    private suspend fun sendAccepts(entry: HistoryEntry): PaxosResult? {
        val changeId = Change.fromHistoryEntry(entry)!!.id
        val acceptedChannel = Channel<PaxosAccepted?>()
        val jobs = scheduleAccepts(acceptedChannel) {
            protocolClient.sendAccept(
                it,
                PaxosAccept(entry.serialize(), currentRound, votedFor?.id!!, history.getCurrentEntryId())
            )
        }

        val responses = gatherAccepts(currentRound, acceptedChannel).filterNotNull()
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
        acceptedChannel: Channel<PaxosAccepted?>,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<PaxosAccepted?>
    ) =
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    val peerAddress: PeerAddress = otherConsensusPeers()[it]
                    val response: ConsensusResponse<PaxosAccepted?> = sendMessage(peerAddress)

                    when {
                        response.message == null -> {
                            logger.info("Peer ${peerAddress.peerId} responded with null")
                            acceptedChannel.send(null)
                        }

                        response.message.isTransactionBlocked -> {
                            logger.info("Peer ${peerAddress.peerId} has transaction blocked")
                            acceptedChannel.send(null)
                        }

                        history.containsEntry(response.message.currentEntryId) && response.message.currentEntryId != history.getCurrentEntryId() -> {
                            logger.info("Peer ${peerAddress.peerId} has outdated history")
                            sendBatchCommit(response.message.currentEntryId, peerAddress)
                            acceptedChannel.send(null)
                        }

                        else -> {
                            logger.info("Peer ${peerAddress.peerId} responded with proper message")
                            acceptedChannel.send(response.message)
                        }
                    }
                }
            }
        }

    private suspend fun gatherAccepts(round: Int, acceptedChannel: Channel<PaxosAccepted?>): List<PaxosAccepted?> {
        val responses: MutableList<PaxosAccepted?> = mutableListOf()

        val peers = otherConsensusPeers().size

        while (responses.size < peers) {
            val response = acceptedChannel.receive()

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
            queuedChange.add(
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

    private suspend fun resetFailureDetector(entry: HistoryEntry, change: Change) {
        if (!leaderFailureDetector.isTaskFinished()) {
            queuedChange.add(
                ChangeToBePropagatedToLeader(
                    change,
                    changeIdToCompletableFuture[change.id]!!
                )
            )
            return
        }
        leaderFailureDetector.cancelCounting()
        leaderFailureDetector.startCounting {
            logger.info("Try to finish entry with id: ${entry.getId()}")
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            queuedChange.add(
                ChangeToBePropagatedToLeader(
                    change,
                    changeIdToCompletableFuture[change.id]!!
                )
            )
            becomeLeader("Leader didn't finish entry ${entry.getId()}")
        }
    }

    private fun sendOldChanges() {
        with(CoroutineScope(leaderRequestExecutorService)) {
            launch(MDCContext()) {
                tryPropagatingChangesToLeader()
            }
        }
    }

    private suspend fun sendBatchCommit(entryId: String, peerAddress: PeerAddress) {
        val entries = history.getAllEntriesUntilHistoryEntryId(entryId)
        entries
            .chunked(maxChangesPerMessage)
            .forEach {
                logger.info("Send BatchCommit to peer ${peerAddress.peerId}")
                val msg = PaxosBatchCommit(
                    PaxosResult.COMMIT,
                    it.map { it.serialize() },
                    currentRound,
                    globalPeerId
                )
                protocolClient.sendBatchCommit(peerAddress, msg)
            }
    }


    private fun isNotValidLeader(message: PaxosResponse, round: Int = currentRound): Boolean =
        message.currentRound > round || (message.currentRound == round && message.currentLeaderId != globalPeerId)

    private fun isMessageFromNotLeader(round: Int, leaderId: PeerId) =
        currentRound > round || (currentRound == round && leaderId != votedFor?.id)

    override fun amILeader(): Boolean = votedFor?.elected == true && votedFor?.id == globalPeerId

    private fun isTransactionFinished(entryId: String, changeId: String) =
        history.containsEntry(entryId) || changeIdToCompletableFuture[changeId]?.isDone == true

    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)


    private suspend fun tryPropagatingChangesToLeader() {
        // TODO mutex?
        val votedFor = this.votedFor
        if (votedFor == null || !votedFor.elected) return
        if (queuedChange.size > 0) logger.info("Try to propagate changes")
        while (true) {
            val changeToBePropagated = queuedChange.poll() ?: break

            when {
                changeIdToCompletableFuture[changeToBePropagated.change.id]?.isDone == true -> {

                }
                amILeader() -> {
                    logger.info("Processing a queued change as a leader: ${changeToBePropagated.change}")
                    proposeChangeToLedger(changeToBePropagated.cf, changeToBePropagated.change)
                    if (changeIdToCompletableFuture[changeToBePropagated.change.id]?.isDone == false) return
                }

                else -> {
                    logger.info("Propagating a change to the leader (${votedFor.id}): ${changeToBePropagated.change}")
                    sendRequestToLeader(changeToBePropagated.cf, changeToBePropagated.change)
                }
            }
        }
    }

}

data class PaxosRound(val round: Int, val entry: HistoryEntry, val proposerId: PeerId)
