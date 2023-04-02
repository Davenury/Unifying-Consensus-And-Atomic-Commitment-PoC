package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.common.TransactionBlocker
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class AlvinProtocol(
    private val peersetId: PeersetId,
    private val history: History,
    private val ctx: ExecutorCoroutineDispatcher,
    private var peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val protocolClient: AlvinProtocolClient,
    private val heartbeatTimeout: Duration = Duration.ofSeconds(4),
    private val heartbeatDelay: Duration = Duration.ofMillis(500),
    private val transactionBlocker: TransactionBlocker,
    private val isMetricTest: Boolean
) : AlvinBroadcastProtocol, SignalSubject {
    //  General consesnus
    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()
    private val peerId = peerResolver.currentPeer()
    private val mutex = Mutex()

    //  Alvin specific fields
    private var lastTransactionId = 0
    private val entryIdToAlvinEntry: ConcurrentHashMap<String, AlvinEntry> = ConcurrentHashMap()
    private val entryIdToFailureDetector: ConcurrentHashMap<String, ProtocolTimer> = ConcurrentHashMap()
    private val entryIdToVotesCommit: ConcurrentHashMap<String, List<PeerId>> = ConcurrentHashMap()
    private val entryIdToVotesAbort: ConcurrentHashMap<String, List<PeerId>> = ConcurrentHashMap()
    private var executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val deliveryQueue: PriorityQueue<AlvinEntry> =
        PriorityQueue { o1, o2 -> o1.transactionId.compareTo(o2.transactionId) }
    private val proposeChannel = Channel<RequestResult<AlvinAckPropose>>()
    private val acceptChannel = Channel<RequestResult<AlvinAckAccept>>()
    private val promiseChannel = Channel<RequestResult<AlvinPromise>>()
    private val fastRecoveryChannel = Channel<RequestResult<AlvinFastRecoveryResponse?>>()

    override fun getPeerName() = peerId.toString()

    override suspend fun begin() {
        logger.info("Start alvin protocol")
    }


//  FIXME: Add change notifier and forwarding metrics

    override suspend fun handleProposalPhase(message: AlvinPropose): AlvinAckPropose {
//        logger.info("Handle proposal: ${} $message")
        logger.info("Handle proposal: ${message.entry.deps}")

        val newDeps: List<HistoryEntry>
        mutex.withLock {
            updateEntry(message.entry.toEntry())
            resetFailureDetector(message.entry.toEntry())
            newDeps = deliveryQueue.map { it.entry }
        }
        val newPos = getNextNum(message.peerId)

        signalPublisher.signal(
            Signal.AlvinReceiveProposal,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = Change.fromHistoryEntry(HistoryEntry.deserialize(message.entry.serializedEntry))
        )

        return AlvinAckPropose(newDeps.map { it.serialize() }, newPos)
    }

    override suspend fun handleAcceptPhase(message: AlvinAccept): AlvinAckAccept {
        val entry = message.entry.toEntry()
        val newDeps: List<HistoryEntry>
        val changeId = Change.fromHistoryEntry(entry.entry)!!.id

        logger.info("Handle stable for entry: ${entry.entry.getId()}, deps: ${entry.deps.map { it.getId() }}")

        mutex.withLock {
            checkTransactionBlocker(entry)
            updateEntry(entry)
            resetFailureDetector(entry)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = changeId,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.CONSENSUS,
                    state = "proposed"
                )
            }

            newDeps = entryIdToAlvinEntry
                .values
                .filter { it.transactionId < message.entry.transactionId }
                .map { it.entry }
        }

        return AlvinAckAccept((newDeps + entry.deps).distinct().map { it.serialize() }, message.entry.transactionId)
    }

    override suspend fun handleStable(message: AlvinStable): AlvinAckStable {
        val entry = message.entry.toEntry()
        val entryId = entry.entry.getId()

        logger.info("Handle stable for entry: ${entry.entry.getId()}, deps: ${entry.deps.map { it.getId() }}")
        mutex.withLock {
            checkTransactionBlocker(entry)
            updateEntry(entry)
            entryIdToFailureDetector[entryId]?.cancelCounting()
        }
        deliverTransaction()

        return AlvinAckStable(peerId)
    }

    override suspend fun handlePrepare(message: AlvinAccept): AlvinPromise {
//        logger.info("Handle prepare: $message")
        val messageEntry = message.entry.toEntry()
        val updatedEntry: AlvinEntry
        val entryId = messageEntry.entry.getId()

        logger.info("Handle prepare for entry: ${messageEntry.entry.getId()}, deps: ${messageEntry.deps.map { it.getId() }}")

        mutex.withLock {
            val entry = entryIdToAlvinEntry[entryId]
            if (entry != null && entry.epoch >= message.entry.epoch) {
                logger.info("Prepare message epoch is outdated")
                throw AlvinOutdatedPrepareException(
                    message.entry.epoch,
                    entry.epoch
                )
            }
            updatedEntry = messageEntry.copy(epoch = message.entry.epoch)
            updateEntry(updatedEntry)
        }

        return AlvinPromise(updatedEntry.toDto())
    }

    override suspend fun handleCommit(message: AlvinCommit): AlvinCommitResponse = mutex.withLock {
        val messageEntry = message.entry.toEntry()
        logger.info("Handle commit: ${message.result} from peer: ${message.peerId}, for entry: ${messageEntry.entry.getId()}")

        val change = Change.fromHistoryEntry(messageEntry.entry)!!
        if (changeIdToCompletableFuture[change.id]?.isDone == true)
            return AlvinCommitResponse(getEntryStatus(messageEntry.entry, change.id), peerId)

        checkTransactionBlocker(messageEntry)
        val entryId = messageEntry.entry.getId()
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        entryIdToVotesCommit.putIfAbsent(entryId, listOf())
        entryIdToVotesAbort.putIfAbsent(entryId, listOf())

        val votesContainer = if (message.result == AlvinResult.COMMIT) entryIdToVotesCommit else entryIdToVotesAbort
        votesContainer[entryId] = (votesContainer.getOrDefault(entryId, listOf()) + listOf(message.peerId)).distinct()
        checkVotes(messageEntry,change)

        return AlvinCommitResponse(getEntryStatus(messageEntry.entry, change.id), peerId)
    }

    override suspend fun handleFastRecovery(message: AlvinFastRecovery): AlvinFastRecoveryResponse = mutex.withLock {
        val historyEntry = history.getEntryFromHistory(message.entryId)

        val alvinEntry = entryIdToAlvinEntry[message.entryId]

        return AlvinFastRecoveryResponse(alvinEntry?.toDto(), historyEntry?.serialize())
    }


    override suspend fun getProposedChanges(): List<Change> = mutex.withLock {
        entryIdToAlvinEntry
            .values
            .filter { it.status != AlvinStatus.STABLE }
            .mapNotNull { Change.fromHistoryEntry(it.entry) }
    }

    override suspend fun getAcceptedChanges(): List<Change> = mutex.withLock {
        history
            .toEntryList(true)
            .mapNotNull { Change.fromHistoryEntry(it) }
    }

    override fun getState(): History = history

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override fun otherConsensusPeers(): List<PeerAddress> {
        return peerResolver.getPeersFromPeerset(peersetId).filter { it.peerId != peerId }
    }


    override fun stop() {
        executorService.close()
    }


    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val result = CompletableFuture<ChangeResult>()
        changeIdToCompletableFuture[change.id] = result
        proposeChangeToLedger(result, change)

        return result
    }

    override suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change) {
        val entry = change.toHistoryEntry(peersetId)
        mutex.withLock {
            if (entryIdToAlvinEntry.containsKey(entry.getId()) || history.containsEntry(entry.getId())) {
                logger.info("Already proposed that change: ${entry.getId()}")
                return
            }

            if (transactionBlocker.isAcquired()) {
                logger.info(
                    "Transaction is blocked, timeout transaction"
                )
                result.complete(ChangeResult(ChangeResult.Status.TIMEOUT))
                return
            }

            try {
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, change.id)
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
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, change.id)
                return
            }
        }
        logger.info("Propose change to ledger: $change")
        proposalPhase(change)
    }


    private suspend fun proposalPhase(change: Change) {

        val historyEntry = change.toHistoryEntry(peersetId)
        val myDeps = listOf(history.getCurrentEntry())
        val entry = AlvinEntry(historyEntry, getNextNum(), myDeps)

        logger.info("Starts proposal phase ${historyEntry.getId()}")

        mutex.withLock {
            updateEntry(entry)
        }

        val jobs = scheduleMessages(historyEntry, proposeChannel, entry.epoch) { peerAddress ->
            protocolClient.sendProposal(peerAddress, AlvinPropose(peerId, entry.toDto()))
        }
        val responses: List<AlvinAckPropose> =
            waitForQuorom(historyEntry, jobs, proposeChannel, AlvinStatus.PENDING)

        val newPos = responses.maxOf { it.newPos }
        val newDeps = responses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        signalPublisher.signal(
            Signal.AlvinAfterProposalPhase,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        decisionPhase(historyEntry, newPos, (myDeps + newDeps).distinct())
    }

    private suspend fun decisionPhase(historyEntry: HistoryEntry, pos: Int, deps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(transactionId = pos, deps = deps, status = AlvinStatus.ACCEPTED)
        mutex.withLock {
            updateEntry(entry)
        }
        logger.info("Starts decision phase ${entry.entry.getId()}")

        val jobs = scheduleMessages(historyEntry, acceptChannel, entry.epoch) { peerAddress ->
            protocolClient.sendAccept(peerAddress, AlvinAccept(peerId, entry.toDto()))
        }

        signalPublisher.signal(
            Signal.AlvinAfterAcceptPhase,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = Change.fromHistoryEntry(historyEntry)
        )

        val newResponses = waitForQuorom(historyEntry, jobs, acceptChannel, AlvinStatus.PENDING)

        val newDeps = newResponses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        deliveryPhase(historyEntry, (deps + newDeps).distinct())
    }

    private suspend fun deliveryPhase(historyEntry: HistoryEntry, newDeps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(deps = newDeps, status = AlvinStatus.STABLE)
        mutex.withLock {
            updateEntry(entry)
        }

        logger.info("Starts delivery phase ${entry.entry.getId()}")

        scheduleMessages(historyEntry, null, entry.epoch) { peerAddress ->
            protocolClient.sendStable(peerAddress, AlvinStable(peerId, entry.toDto()))
        }

        signalPublisher.signal(
            Signal.AlvinAfterStablePhase,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = Change.fromHistoryEntry(historyEntry)
        )

        deliverTransaction()
    }

    private suspend fun recoveryPhase(entry: AlvinEntry) {

        logger.info("Starts recovery phase for ${entry.entry.getId()}")

        var newEntry = entry.copy(epoch = entry.epoch + 1, status = AlvinStatus.UNKNOWN)
        mutex.withLock {
            updateEntry(newEntry)
        }
        val jobs = scheduleMessages(entry.entry, promiseChannel, newEntry.epoch) { peerAddress ->
            protocolClient.sendPrepare(
                peerAddress,
                AlvinAccept(peerId, newEntry.toDto())
            )
        }

        val responses = waitForQuorom(entry.entry, jobs, promiseChannel, AlvinStatus.UNKNOWN, includeMyself = false)

        val newDeps = responses
            .filter { it.entry != null }
            .flatMap { it.entry!!.deps }
            .distinct()
        val newPos =
            responses.filter { it.entry != null }.maxOfOrNull { it.entry!!.transactionId } ?: entry.transactionId

        newEntry = newEntry.copy(transactionId = newPos, deps = newDeps.map { HistoryEntry.deserialize(it) })

        when {
            responses.any { it.entry?.status == AlvinStatus.STABLE } -> {
                logger.info("Entry is in Stable status, start delivery phase")
                newEntry = newEntry.copy(status = AlvinStatus.STABLE)
                mutex.withLock {
                    updateEntry(newEntry)
                }
                deliveryPhase(newEntry.entry, newEntry.deps)
            }

            responses.any { it.entry?.status == AlvinStatus.ACCEPTED } -> {
                logger.info("Entry is in Accepted status, start decision phase")
                newEntry = newEntry.copy(status = AlvinStatus.ACCEPTED)
                mutex.withLock {
                    updateEntry(newEntry)
                }
                decisionPhase(newEntry.entry, newEntry.transactionId, newEntry.deps)
            }

            else -> {
                logger.info("Entry is in no status, start proposal phase")
                newEntry = newEntry.copy(status = AlvinStatus.PENDING)
                mutex.withLock {
                    updateEntry(newEntry)
                }
                proposalPhase(Change.fromHistoryEntry(newEntry.entry)!!)
            }
        }
    }


    private suspend fun checkTransactionBlocker(entry: AlvinEntry) {
        val changeId = Change.fromHistoryEntry(entry.entry)!!.id
        when {
            isBlockedOnDifferentProtocol() ->
                throw AlvinHistoryBlocked(transactionBlocker.getChangeId()!!, transactionBlocker.getProtocolName()!!)

            !transactionBlocker.isAcquired() && changeIdToCompletableFuture[changeId]?.isDone != true ->
                transactionBlocker.tryToBlock(ProtocolName.CONSENSUS, Change.fromHistoryEntry(entry.entry)!!.id)
        }
    }

    private suspend fun <A> scheduleMessages(
        entry: HistoryEntry,
        channel: Channel<RequestResult<A>>?,
        epoch: Int,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<A?>
    ) = scheduleMessages(entry.getId(), channel, epoch, sendMessage)


    private suspend fun <A> scheduleMessages(
        entryId: String,
        channel: Channel<RequestResult<A>>?,
        epoch: Int,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<A?>
    ) =
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    var peerAddress: PeerAddress
                    var response: ConsensusResponse<A?>
                    do {
                        peerAddress = otherConsensusPeers()[it]
                        response = sendMessage(peerAddress)

                        val entry: AlvinEntry?
                        mutex.withLock {
                            entry = entryIdToAlvinEntry[entryId]
                        }
                        if (entry != null && entry.epoch > epoch) {
                            logger.info("Our entry epoch increased so stop sending message, scheduleMessage epoch ${epoch}, new epoch: ${entry.epoch}")
                            resetFailureDetector(entry)
                            return@launch
                        }

                        if (entry != null && response.unauthorized) {
                            logger.info("Peer responded that our epoch is outdated, wait for newer messages")
                            resetFailureDetector(entry)
                            return@launch
                        }

                        if (response.message == null && !response.unauthorized) delay(heartbeatDelay.toMillis())

                    } while (response.message == null && !response.unauthorized)

                    if (response.message != null) channel?.send(
                        RequestResult(
                            entryId,
                            response.message!!,
                            response.unauthorized
                        )
                    )
                }
            }
        }

    private suspend fun <A> waitForQuorom(
        historyEntry: HistoryEntry,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus,
        includeMyself: Boolean = true
    ): List<A> = waitForQuorom<A>(historyEntry.getId(), jobs, channel, status, includeMyself)

    private suspend fun <A> waitForQuorom(
        entryId: String,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus,
        includeMyself: Boolean = true
    ): List<A> {
        val responses: MutableList<A> = mutableListOf()
        val changeSize = if (includeMyself) 0 else -1
        while (!isMoreThanHalf(responses.size + changeSize)) {
            val response = channel.receive()
            val entry = entryIdToAlvinEntry[entryId]

            when {
                response.entryId == entryId && !response.unauthorized -> responses.add(response.response)
                response.entryId == entryId -> {
                    resetFailureDetector(entry!!)
                    throw AlvinLeaderBecameOutdatedException(Change.fromHistoryEntry(entry.entry)!!.id)
                }

                status == entry?.status && status != AlvinStatus.UNKNOWN -> channel.send(response)
            }
        }

        jobs.forEach { if (it.isActive) it.cancel() }

        return responses
    }

    private suspend fun <A> scheduleMessagesOnce(
        entryId: String,
        channel: Channel<RequestResult<A?>>,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<A?>
    ) =
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    val peerAddress = otherConsensusPeers()[it]
                    val response = sendMessage(peerAddress)
                    channel.send(RequestResult(entryId, response.message))
                }
            }
        }

    private suspend fun <A> gatherResponses(
        entryId: String,
        channel: Channel<RequestResult<A>>,
    ): List<A> {
        val responses: MutableList<A> = mutableListOf()
        while (responses.size < otherConsensusPeers().size) {
            val response = channel.receive()
            if (response.entryId == entryId) responses.add(response.response)
            else channel.send(response)
        }

        return responses
    }


    private suspend fun isBlockedOnDifferentProtocol() =
        transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() != ProtocolName.CONSENSUS

    //  TODO: Check if any transaction from delivery queue can be commited or aborted
//  If can be committed send commit message to all other peers and wait for qurom commit messages.
    private suspend fun deliverTransaction() {

        var resultEntry: AlvinEntry
        do {
            if (deliveryQueue.size == 0) {
                logger.info("Delivered all transactions")
                return
            }
            val entry = cleanDeps(deliveryQueue.poll())
            val changeId = Change.fromHistoryEntry(entry.entry)!!.id
            resultEntry = entry
            if (history.containsEntry(entry.entry.getId())) {
                scheduleCommitMessages(entry, AlvinResult.COMMIT)
            }else if(changeIdToCompletableFuture[changeId]?.isDone == true){
                scheduleCommitMessages(entry, AlvinResult.ABORT)
            }
        } while (changeIdToCompletableFuture[changeId]?.isDone == true)


        val entry: AlvinEntry = resultEntry
        val changeId = Change.fromHistoryEntry(entry.entry)!!.id

        if (history.containsEntry(entry.entry.getId())) {
            scheduleCommitMessages(entry, AlvinResult.COMMIT)
            return
        }else if(changeIdToCompletableFuture[changeId]?.isDone == true){
            scheduleCommitMessages(entry, AlvinResult.ABORT)
            return
        }

        if (entry.status != AlvinStatus.STABLE) {
            logger.info("Entry is not stable yet (id: ${entry.entry.getId()}")
            deliveryQueue.add(entry)
            return
        }

        val depsResult = entry.deps.map {
            isEntryFinished(it.getId())
        }


        when {
            depsResult.all { it } && !history.isEntryCompatible(entry.entry) -> {
                logger.info("Entry ${entry.entry.getId()} is incompatible, send abort")
                scheduleCommitMessages(entry, AlvinResult.ABORT)
            }

            depsResult.all { it } -> {
                logger.info("Entry ${entry.entry.getId()} is compatible, send commit")
                scheduleCommitMessages(entry, AlvinResult.COMMIT)
            }

            else -> {
                logger.info("Some previous change isn't finished yet wait for it")
                deliveryQueue.add(entry)
            }
        }
    }

    private suspend fun scheduleCommitMessages(entry: AlvinEntry, result: AlvinResult) {
        scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
            val response = protocolClient.sendCommit(peerAddress, AlvinCommit(entry.toDto(), result, peerId))
            if(response.message != null) updateVotes(entry,response.message)
            response
        }
    }

    private suspend fun updateVotes(entry: AlvinEntry, response: AlvinCommitResponse): Unit = mutex.withLock {
        val change = Change.fromHistoryEntry(entry.entry)!!
        val entryId = entry.entry.getId()

        if(changeIdToCompletableFuture[change.id]?.isDone == true) return

        val entryIdToVotes = when (response.result) {
            AlvinResult.COMMIT -> entryIdToVotesCommit
            AlvinResult.ABORT -> entryIdToVotesAbort
            else -> null
        } ?: return@withLock

        entryIdToVotes[entryId] = (entryIdToVotes.getOrDefault(entryId, listOf()) + listOf(response.peerId)).distinct()
        checkVotes(entry, change)
    }


//  Mutex function
    private suspend fun checkVotes(entry: AlvinEntry, change: Change) {
        val entryId = entry.entry.getId()
        val myselfVotesForCommit = history.isEntryCompatible(entry.entry)

        val commitVotes = entryIdToVotesCommit.getOrDefault(entryId, listOf()).distinct().size
        val abortVotes = entryIdToVotesAbort.getOrDefault(entryId, listOf()).distinct().size


        val commitDecision = if (myselfVotesForCommit) isMoreThanHalf(commitVotes) else isMoreThanHalf(commitVotes - 1)
        val abortDecision = if (myselfVotesForCommit) isMoreThanHalf(abortVotes - 1) else isMoreThanHalf(abortVotes)

        if ((commitDecision || abortDecision) && changeIdToCompletableFuture[change.id]?.isDone != true) {
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            entryIdToAlvinEntry.remove(entryId)
            entryIdToFailureDetector[entryId]?.cancelCounting()
            entryIdToFailureDetector.remove(entryId)
            entryIdToVotesCommit.remove(entryId)
            entryIdToVotesAbort.remove(entryId)

            val (changeResult, signal) = if (commitDecision) {
                if (isMetricTest) {
                    Metrics.bumpChangeMetric(
                        changeId = change.id,
                        peerId = peerId,
                        peersetId = peersetId,
                        protocolName = ProtocolName.CONSENSUS,
                        state = "accepted"
                    )
                }
                history.addEntry(entry.entry)
                Pair(ChangeResult.Status.SUCCESS, Signal.AlvinCommitChange)
            } else {
                Pair(ChangeResult.Status.CONFLICT, Signal.AlvinAbortChange)
            }
            logger.info("The result of change (${change.id}) is $changeResult")
            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(changeResult))
            signalPublisher.signal(signal, this, mapOf(peersetId to otherConsensusPeers()), change = change)
            if (transactionBlocker.getChangeId() == change.id) transactionBlocker.tryToReleaseBlockerChange(
                ProtocolName.CONSENSUS,
                change.id
            )
        }
    }


    //  FIXME: we should only wait until all deps are finished not necessarily committed
    private suspend fun isEntryFinished(entryId: String): Boolean {
        if (history.containsEntry(entryId)) return true
        if (entryIdToAlvinEntry[entryId] != null) {
            resetFailureDetector(entryIdToAlvinEntry[entryId]!!)
            return false
        }

//      epoch 1 because we don't current value
        scheduleMessagesOnce(entryId, fastRecoveryChannel) {
            protocolClient.sendFastRecovery(it, AlvinFastRecovery(entryId))
        }

        val responses: List<AlvinFastRecoveryResponse?> = gatherResponses(entryId, fastRecoveryChannel)

        val historyEntry =
            responses.find { it?.historyEntry != null }?.historyEntry?.let { HistoryEntry.deserialize(it) }
        val alvinEntry = responses.find { it?.entry != null }?.entry?.toEntry()

        return when {
            historyEntry != null && isEntryFinished(historyEntry.getParentId()!!) ->
                mutex.withLock {
                    logger.info("Add missing history entry: ${historyEntry.getId()}")
                    history.addEntry(historyEntry)
                    true
                }

            alvinEntry != null -> {
                resetFailureDetector(alvinEntry)
                false
            }

            else -> true
        }
    }


    private fun cleanDeps(entry: AlvinEntry): AlvinEntry {
        val finalDeps = entry.deps.filter {
            val alvinEntry = entryIdToAlvinEntry[it.getId()]
            alvinEntry != null && alvinEntry.transactionId < entry.transactionId
        }

        val unknownDeps = entry.deps.filter { !history.containsEntry(it.getId()) && it.getId() != entry.entry.getId() }

        return entry.copy(deps = unknownDeps + finalDeps)
    }

    //  mutex function
    private suspend fun resetFailureDetector(entry: AlvinEntry) {
        val entryId = entry.entry.getId()
        entryIdToFailureDetector[entryId]?.cancelCounting()
        entryIdToFailureDetector.putIfAbsent(entryId, getFailureDetectorTimer())
        if (entryIdToAlvinEntry.containsKey(entryId)) entryIdToFailureDetector[entryId]!!.startCounting(entry.epoch) {
            recoveryPhase(entry)
        }
    }

    //  Mutex function
    private fun updateEntry(entry: AlvinEntry) {
        val entryId = entry.entry.getId()
        val oldEntry = entryIdToAlvinEntry[entryId]
        val changeId: String = Change.fromHistoryEntry(entry.entry)!!.id

        if (changeIdToCompletableFuture[changeId]?.isDone == true && entry.status != AlvinStatus.STABLE) return

        var updatedEntry = entry
        if (oldEntry?.status == AlvinStatus.STABLE) {
            updatedEntry = updatedEntry.copy(status = AlvinStatus.STABLE)
        }

        if (updatedEntry.status == AlvinStatus.STABLE || oldEntry?.status == AlvinStatus.PENDING || oldEntry?.status == AlvinStatus.UNKNOWN || oldEntry?.status == null) {
            entryIdToAlvinEntry[entryId] = updatedEntry
            deliveryQueue.removeIf { it.entry == updatedEntry.entry }
            deliveryQueue.add(updatedEntry)
        }
    }

    private fun peers() = peerResolver.getPeersFromPeerset(peersetId)

    private fun getEntryStatus(entry: HistoryEntry, changeId: String): AlvinResult? = when {
        history.containsEntry(entry.getId()) -> AlvinResult.COMMIT
        changeIdToCompletableFuture[changeId]?.isDone == true -> AlvinResult.ABORT
        else -> null
    }


    private suspend fun getNextNum(peerId: PeerId = this.peerId): Int = mutex.withLock {
        val addresses = peerResolver.getPeersFromPeerset(peersetId).sortedBy { it.peerId.peerId }
        val peerAddress = peerResolver.resolve(peerId)
        val index = addresses.indexOf(peerAddress)

        val previousMod = lastTransactionId % peers().size
        val removeMod = lastTransactionId - previousMod
        lastTransactionId = if (index > previousMod)
            removeMod + index
        else
            removeMod + peers().size + index

        return lastTransactionId
    }

    private fun getFailureDetectorTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)


    companion object {
        private val logger = LoggerFactory.getLogger("alvin")
    }


}

data class AlvinEntry(
    val entry: HistoryEntry,
    val transactionId: Int,
    val deps: List<HistoryEntry>,
    val epoch: Int = 0,
    val status: AlvinStatus = AlvinStatus.PENDING
) {
    fun toDto() = AlvinEntryDto(entry.serialize(), transactionId, deps.map { it.serialize() }, epoch, status)

}

data class AlvinEntryDto(
    val serializedEntry: String,
    val transactionId: Int,
    val deps: List<String>,
    val epoch: Int = 0,
    val status: AlvinStatus = AlvinStatus.PENDING
) {
    fun toEntry() = AlvinEntry(
        HistoryEntry.deserialize(serializedEntry),
        transactionId,
        deps.map { HistoryEntry.deserialize(it) },
        epoch,
        status
    )
}

data class RequestResult<A>(
    val entryId: String,
    val response: A,
    val unauthorized: Boolean = false
)



enum class AlvinStatus {
    PENDING, ACCEPTED, STABLE, UNKNOWN
}



