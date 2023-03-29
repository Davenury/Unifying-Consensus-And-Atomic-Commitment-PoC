package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
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
    private val globalPeerId = peerResolver.currentPeer()
    private val mutex = Mutex()

    //  Alvin specific fields
    private var lastTransactionId = 0
    private val entryIdToAlvinEntry: ConcurrentHashMap<String, AlvinEntry> = ConcurrentHashMap()
    private val entryIdToFailureDetector: ConcurrentHashMap<String, ProtocolTimer> = ConcurrentHashMap()
    private val entryIdToVotesCommit: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
    private val entryIdToVotesAbort: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
    private var executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val deliveryQueue: PriorityQueue<AlvinEntry> =
        PriorityQueue { o1, o2 -> o1.transactionId.compareTo(o2.transactionId) }
    private val proposeChannel = Channel<RequestResult<AlvinAckPropose>>()
    private val acceptChannel = Channel<RequestResult<AlvinAckAccept>>()
    private val promiseChannel = Channel<RequestResult<AlvinPromise>>()
    private val fastRecoveryChannel = Channel<RequestResult<AlvinFastRecoveryResponse?>>()

    override fun getPeerName() = globalPeerId.toString()

    override suspend fun begin() {}



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
//        logger.info("Handle accept: $message")
        logger.info("Handle accept: ${message.entry.deps}")
        val entry = message.entry.toEntry()
        val newDeps: List<HistoryEntry>


        mutex.withLock {
            checkTransactionBlocker(entry)
            updateEntry(entry)
            resetFailureDetector(entry)


            newDeps = entryIdToAlvinEntry
                .values
                .filter { it.transactionId < message.entry.transactionId }
                .map { it.entry }
        }

        return AlvinAckAccept((newDeps + entry.deps).distinct().map { it.serialize() }, message.entry.transactionId)
    }

    override suspend fun handleStable(message: AlvinStable): AlvinAckStable {
//        logger.info("Handle stable: $message")
        logger.info("Handle stable: ${message.entry.deps.map { HistoryEntry.deserialize(it).getId() }}")
        val entry = message.entry.toEntry()
        val entryId = entry.entry.getId()
        mutex.withLock {
            checkTransactionBlocker(entry)
            updateEntry(entry)
            entryIdToFailureDetector[entryId]?.cancelCounting()
        }
        deliverTransaction()

        return AlvinAckStable(globalPeerId)
    }

    override suspend fun handlePrepare(message: AlvinAccept): AlvinPromise {
//        logger.info("Handle prepare: $message")
        logger.info("Handle prepare: ${message.entry.deps}")
        val messageEntry = message.entry.toEntry()
        val updatedEntry: AlvinEntry
        val entryId = messageEntry.entry.getId()
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

    override suspend fun handleCommit(message: AlvinCommit): Unit = mutex.withLock {
        val messageEntry = message.entry.toEntry()

        val change = Change.fromHistoryEntry(messageEntry.entry)!!
        if (changeIdToCompletableFuture[change.id]?.isDone == true) return
        checkTransactionBlocker(messageEntry)

        val entryId = messageEntry.entry.getId()
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())

        entryIdToVotesCommit.putIfAbsent(entryId, 0)
        entryIdToVotesAbort.putIfAbsent(entryId, 0)

        val votesContainer = if (message.result == AlvinResult.COMMIT) entryIdToVotesCommit else entryIdToVotesAbort
        votesContainer[entryId] = votesContainer.getOrDefault(entryId, 0) + 1

        val myselfVotesForCommit = history.isEntryCompatible(messageEntry.entry)

        val commitVotes = entryIdToVotesCommit[entryId]!!
        val abortVotes = entryIdToVotesAbort[entryId]!!

        logger.info("Handle commit: ${message.result} from peer: ${message.peerId}, votes: ($commitVotes,$abortVotes) for entry $entryId")

        val commitDecision = if (myselfVotesForCommit) isMoreThanHalf(commitVotes) else isMoreThanHalf(commitVotes - 1)
        val abortDecision = if (myselfVotesForCommit) isMoreThanHalf(abortVotes - 1) else isMoreThanHalf(abortVotes)

        if ((commitDecision || abortDecision) && !changeIdToCompletableFuture[change.id]!!.isDone) {
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            entryIdToAlvinEntry.remove(entryId)
            entryIdToFailureDetector[entryId]?.cancelCounting()
            entryIdToFailureDetector.remove(entryId)
            votesContainer.remove(entryId)

            val (changeResult, signal) = if (commitDecision) {
                history.addEntry(messageEntry.entry)
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

//        TODO after commiting / aborting change check if there are not waiting for committing
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
        return peerResolver.getPeersFromPeerset(peersetId).filter { it.peerId != globalPeerId }
    }


    override fun stop() {
        executorService.close()
    }

    @Deprecated("use proposeChangeAsync")
    override suspend fun proposeChange(change: Change): ChangeResult = proposeChangeAsync(change).await()

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
                logger.info("Already proposed that change: $change")
                return
            }

            if (transactionBlocker.isAcquired()) {
                logger.info(
                    "Queued change, because: transaction is blocked"
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
        logger.info("Starts proposal phase $change")
        val historyEntry = change.toHistoryEntry(peersetId)
        val myDeps = listOf(history.getCurrentEntry())
        val entry = AlvinEntry(historyEntry, getNextNum(), myDeps)

        mutex.withLock {
            updateEntry(entry)
        }

        val jobs = scheduleMessages(historyEntry, proposeChannel, entry.epoch) { peerAddress ->
            protocolClient.sendProposal(peerAddress, AlvinPropose(globalPeerId, entry.toDto()))
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
        logger.info("Starts decision phase $entry")

        val jobs = scheduleMessages(historyEntry, acceptChannel, entry.epoch) { peerAddress ->
            protocolClient.sendAccept(peerAddress, AlvinAccept(globalPeerId, entry.toDto()))
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

        logger.info("Starts delivery phase $entry")

        scheduleMessages(historyEntry, null, entry.epoch) { peerAddress ->
            protocolClient.sendStable(peerAddress, AlvinStable(globalPeerId, entry.toDto()))
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

        logger.info("Starts recovery phase for $entry")

        var newEntry = entry.copy(epoch = entry.epoch + 1, status = AlvinStatus.UNKNOWN)
        updateEntry(newEntry)

        logger.info("Send prepare messages")

        val jobs = scheduleMessages(entry.entry, promiseChannel, newEntry.epoch) { peerAddress ->
            protocolClient.sendPrepare(
                peerAddress,
                AlvinAccept(globalPeerId, newEntry.toDto())
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


    private fun checkTransactionBlocker(entry: AlvinEntry) {
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


    private fun isBlockedOnDifferentProtocol() =
        transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() != ProtocolName.CONSENSUS

    //  TODO: Check if any transaction from delivery queue can be commited or aborted
//  If can be committed send commit message to all other peers and wait for qurom commit messages.
    private suspend fun deliverTransaction() {

        var resultEntry: AlvinEntry
        do {
            if(deliveryQueue.size == 0){
                logger.info("Delivered all transactions")
                return
            }
            val entry = cleanDeps(deliveryQueue.poll())
            resultEntry = entry
            if(history.containsEntry(entry.entry.getId())) {
                scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                    protocolClient.sendCommit(
                        peerAddress,
                        AlvinCommit(entry.toDto(), AlvinResult.COMMIT, globalPeerId)
                    )
                }
            }
        }while (history.containsEntry(entry.entry.getId()))


        val entry: AlvinEntry = resultEntry

        if (history.containsEntry(entry.entry.getId())) {
            scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                protocolClient.sendCommit(
                    peerAddress,
                    AlvinCommit(entry.toDto(), AlvinResult.COMMIT, globalPeerId)
                )
            }
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


        logger.info("EntryId: ${entry.entry.getId()}, its deps: ${entry.deps.map { it.getId() }}")

        when {
            depsResult.all { it } && !history.isEntryCompatible(entry.entry) -> {
                scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                    protocolClient.sendCommit(
                        peerAddress,
                        AlvinCommit(entry.toDto(), AlvinResult.ABORT, globalPeerId)
                    )
                }
            }

            depsResult.all { it } -> {
                scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                    protocolClient.sendCommit(
                        peerAddress,
                        AlvinCommit(entry.toDto(), AlvinResult.COMMIT, globalPeerId)
                    )
                }
            }

            else -> {
                logger.info("Some previous change isn't finished yet wait for it $depsResult")
                deliveryQueue.add(entry)
            }
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
        if(entryIdToAlvinEntry.containsKey(entryId))entryIdToFailureDetector[entryId]!!.startCounting(entry.epoch) {
            recoveryPhase(entry)
        }
    }

    //  Mutex function
    private fun updateEntry(entry: AlvinEntry) {
        val entryId = entry.entry.getId()
        val oldEntry = entryIdToAlvinEntry[entryId]
        val changeId: String = Change.fromHistoryEntry(entry.entry)!!.id

        if(changeIdToCompletableFuture[changeId]?.isDone == true && entry.status != AlvinStatus.STABLE) return

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




    private suspend fun getNextNum(peerId: PeerId = globalPeerId): Int = mutex.withLock {
        val addresses = peerResolver.getPeersFromPeerset(peersetId)
        val peerAddress = peerResolver.resolve(peerId)
        val index = addresses.indexOf(peerAddress)

        val previousMod = lastTransactionId % peers().size
        val removeMod = lastTransactionId - previousMod
        if (index > previousMod)
            lastTransactionId = removeMod + index
        else
            lastTransactionId = removeMod + peers().size + index

        return lastTransactionId
    }

    private fun getFailureDetectorTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)

    //  TODO: remove it
    override fun setPeerAddress(address: String) {
    }


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


