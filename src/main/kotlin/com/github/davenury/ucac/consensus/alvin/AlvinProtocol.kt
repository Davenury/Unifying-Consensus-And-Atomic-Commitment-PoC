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
    private val history: History,
    private val ctx: ExecutorCoroutineDispatcher,
    private var peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
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
    private val entryIdToVotes: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
    private var executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val deliveryQueue: PriorityQueue<AlvinEntry> =
        PriorityQueue { o1, o2 -> o1.transactionId.compareTo(o2.transactionId) }
    private val proposeChannel = Channel<RequestResult<AlvinAckPropose>>()
    private val acceptChannel = Channel<RequestResult<AlvinAckAccept>>()
    private val promiseChannel = Channel<RequestResult<AlvinPromise>>()
    private val fastRecoveryChannel = Channel<RequestResult<AlvinFastRecoveryResponse?>>()

    override fun getPeerName() = globalPeerId.toString()

    override suspend fun begin() {}


//  TODO: use transactionBlocker

    override suspend fun handleProposalPhase(message: AlvinPropose): AlvinAckPropose {
//        logger.info("Handle proposal: ${} $message")
        logger.info("Handle proposal: ${message.entry.deps}")

        val newDeps: List<HistoryEntry>
        resetFailureDetector(message.entry.toEntry())
        updateEntry(message.entry.toEntry())
        mutex.withLock {
            newDeps = deliveryQueue.map { it.entry }
        }
        val newPos = getNextNum(message.peerId)

        signalPublisher.signal(
            Signal.AlvinReceiveProposal,
            this,
            listOf(otherConsensusPeers()),
            change = Change.fromHistoryEntry(HistoryEntry.deserialize(message.entry.serializedEntry))
        )

        return AlvinAckPropose(newDeps.map { it.serialize() }, newPos)
    }

    override suspend fun handleAcceptPhase(message: AlvinAccept): AlvinAckAccept {
//        logger.info("Handle accept: $message")
        logger.info("Handle accept: ${message.entry.deps}")
        val entry = message.entry.toEntry()

        checkTransactionBlocker(entry)

        resetFailureDetector(entry)
        updateEntry(entry)
        val newDeps: List<HistoryEntry>
        mutex.withLock {
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
        checkTransactionBlocker(entry)

        entryIdToFailureDetector[entryId]?.cancelCounting()
        updateEntry(entry)
        deliverTransaction()

        return AlvinAckStable(globalPeerId.peerId)
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
            entryIdToAlvinEntry[entryId] = updatedEntry
        }
        updateEntry(updatedEntry)
        return AlvinPromise(updatedEntry.toDto())
    }

    override suspend fun handleCommit(message: AlvinCommit): Unit = mutex.withLock {
        logger.info("Handle commit: $message")
        val messageEntry = message.entry.toEntry()

        val change = Change.fromHistoryEntry(messageEntry.entry)!!
        if (changeIdToCompletableFuture[change.id]?.isDone == true) return
        checkTransactionBlocker(messageEntry)

        val entryId = messageEntry.entry.getId()
        var votes: Int = entryIdToVotes[entryId] ?: 0
        if (message.result == AlvinResult.COMMIT) {
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            votes = (entryIdToVotes[entryId] ?: 0) + 1
            entryIdToVotes[entryId] = votes
        }
        val isCommitted = isMoreThanHalf(votes)

        if (isCommitted || message.result == AlvinResult.ABORT) {
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            entryIdToAlvinEntry.remove(entryId)
            entryIdToFailureDetector[entryId]?.cancelCounting()
            entryIdToFailureDetector.remove(entryId)
            entryIdToVotes.remove(entryId)

            val (changeResult, signal) = if (isCommitted) {
                history.addEntry(messageEntry.entry)
                Pair(ChangeResult.Status.SUCCESS, Signal.AlvinCommitChange)
            } else {
                Pair(ChangeResult.Status.CONFLICT, Signal.AlvinAbortChange)
            }
            logger.info("The result of change (${change.id}) is $changeResult")
            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(changeResult))
            signalPublisher.signal(signal, this, listOf(otherConsensusPeers()), change = change)
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.CONSENSUS, change.id)
        }
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
        return peerResolver.getPeersFromCurrentPeerset().filter { it.globalPeerId != globalPeerId }
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
        val entry = change.toHistoryEntry(globalPeerId.peersetId)
        mutex.withLock {
            if (entryIdToAlvinEntry.containsKey(entry.getId())) {
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
                        change.toHistoryEntry(peerResolver.currentPeer().peersetId).getParentId()
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
        val historyEntry = change.toHistoryEntry(globalPeerId.peersetId)
        val myDeps = listOf(history.getCurrentEntry())
        val entry = AlvinEntry(historyEntry, getNextNum(), myDeps)
        updateEntry(entry)

        val jobs = scheduleMessages(historyEntry, proposeChannel, entry.epoch) { peerAddress ->
            protocolClient.sendProposal(peerAddress, AlvinPropose(globalPeerId.peerId, entry.toDto()))
        }
        val responses: List<AlvinAckPropose> = waitForQurom(historyEntry, jobs, proposeChannel, AlvinStatus.PENDING)

        val newPos = responses.maxOf { it.newPos }
        val newDeps = responses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        signalPublisher.signal(
            Signal.AlvinAfterProposalPhase,
            this,
            listOf(peers()),
            change = change
        )

        decisionPhase(historyEntry, newPos, (myDeps + newDeps).distinct())
    }

    private suspend fun decisionPhase(historyEntry: HistoryEntry, pos: Int, deps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(transactionId = pos, deps = deps, status = AlvinStatus.ACCEPTED)
        updateEntry(entry)
        logger.info("Starts decision phase $entry")

        val jobs = scheduleMessages(historyEntry, acceptChannel, entry.epoch) { peerAddress ->
            protocolClient.sendAccept(peerAddress, AlvinAccept(globalPeerId.peerId, entry.toDto()))
        }

        signalPublisher.signal(
            Signal.AlvinAfterAcceptPhase,
            this,
            listOf(peers()),
            change = Change.fromHistoryEntry(historyEntry)
        )

        val newResponses = waitForQurom(historyEntry, jobs, acceptChannel, AlvinStatus.PENDING)

        val newDeps = newResponses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        deliveryPhase(historyEntry, (deps + newDeps).distinct())
    }

    private suspend fun deliveryPhase(historyEntry: HistoryEntry, newDeps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(deps = newDeps, status = AlvinStatus.STABLE)
        updateEntry(entry)

        logger.info("Starts delivery phase $entry")

        scheduleMessages(historyEntry, null, entry.epoch) { peerAddress ->
            protocolClient.sendStable(peerAddress, AlvinStable(globalPeerId.peerId, entry.toDto()))
        }

        signalPublisher.signal(
            Signal.AlvinAfterStablePhase,
            this,
            listOf(peers()),
            change = Change.fromHistoryEntry(historyEntry)
        )

        deliverTransaction()
    }

    private suspend fun recoveryPhase(entry: AlvinEntry) {

        logger.info("Starts recovery phase for $entry")

        val entryId = entry.entry.getId()
        var newEntry = entry.copy(epoch = entry.epoch + 1, status = AlvinStatus.UNKNOWN)
        entryIdToAlvinEntry[entryId] = newEntry

        logger.info("Send prepare messages")

        val jobs = scheduleMessages(entry.entry, promiseChannel, newEntry.epoch) { peerAddress ->
            protocolClient.sendPrepare(
                peerAddress,
                AlvinAccept(globalPeerId.peerId, newEntry.toDto())
            )
        }

        val responses = waitForQurom(entry.entry, jobs, promiseChannel, AlvinStatus.UNKNOWN)

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
                updateEntry(newEntry)
                deliveryPhase(newEntry.entry, newEntry.deps)
            }

            responses.any { it.entry?.status == AlvinStatus.ACCEPTED } -> {
                logger.info("Entry is in Accepted status, start decision phase")
                newEntry = newEntry.copy(status = AlvinStatus.ACCEPTED)
                updateEntry(newEntry)
                decisionPhase(newEntry.entry, newEntry.transactionId, newEntry.deps)
            }

            else -> {
                logger.info("Entry is in no status, start proposal phase")
                newEntry = newEntry.copy(status = AlvinStatus.PENDING)
                updateEntry(newEntry)
                proposalPhase(Change.fromHistoryEntry(newEntry.entry)!!)
            }
        }
    }


    private fun checkTransactionBlocker(entry: AlvinEntry) {
        when {
            isBlockedOnDifferentProtocol() ->
                throw AlvinHistoryBlocked(transactionBlocker.getChangeId()!!)

            !transactionBlocker.isAcquired() ->
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
        (0 until getOtherPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    var peerAddress: PeerAddress
                    var response: ConsensusResponse<A?>
                    do {
                        delay(heartbeatDelay.toMillis())
                        peerAddress = getOtherPeers()[it]
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

                        if (entry != null && response.unathorized) {
                            logger.info("Peer responded that our epoch is outdated, wait for newer messages")
                            resetFailureDetector(entry)
                            return@launch
                        }

                    } while (response.message == null && !response.unathorized)

                    if (response.message != null) channel?.send(
                        RequestResult(
                            entryId,
                            response.message!!,
                            response.unathorized
                        )
                    )
                }
            }
        }

    private suspend fun <A> waitForQurom(
        historyEntry: HistoryEntry,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus
    ): List<A> = waitForQurom<A>(historyEntry.getId(), jobs, channel, status)

    private suspend fun <A> waitForQurom(
        entryId: String,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus
    ): List<A> {
        val responses: MutableList<A> = mutableListOf()
        while (!isMoreThanHalf(responses.size)) {
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
        (0 until getOtherPeers().size).map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    val peerAddress = getOtherPeers()[it]
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
        val entry = cleanDeps(deliveryQueue.poll())

        if (entry.status != AlvinStatus.STABLE) {
            logger.info("Entry is not stable yet")
            deliveryQueue.add(entry)
            return
        }

        val depsResult = entry.deps.map {
            isEntryCommitted(it.getId())
        } + listOf(history.isEntryCompatible(entry.entry))


        logger.info("EntryId: ${entry.entry.getId()}, its deps: ${entry.deps.map { it.getId() }}")

        when {
            depsResult.any { it == false } -> {
                scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                    protocolClient.sendCommit(peerAddress, AlvinCommit(entry.toDto(), AlvinResult.ABORT))
                }
            }

            depsResult.all { it == true } -> {
                scheduleMessages(entry.entry, null, entry.epoch) { peerAddress ->
                    protocolClient.sendCommit(peerAddress, AlvinCommit(entry.toDto(), AlvinResult.COMMIT))
                }
            }

            else -> {
                logger.info("Some previous change isn't finished yet wait for it $depsResult")
                deliveryQueue.add(entry)
            }
        }
    }

    private suspend fun isEntryCommitted(entryId: String): Boolean? {
        if (history.containsEntry(entryId)) return true
        if (entryIdToAlvinEntry[entryId] != null) {
            resetFailureDetector(entryIdToAlvinEntry[entryId]!!)
            return null
        }

//      epoch 1 because we don't current value
        scheduleMessagesOnce(entryId, fastRecoveryChannel) {
            protocolClient.sendFastRecovery(it, AlvinFastRecovery(entryId))
        }

        val responses = gatherResponses(entryId, fastRecoveryChannel)

        val historyEntry =
            responses.find { it?.historyEntry != null }?.historyEntry?.let { HistoryEntry.deserialize(it) }
        val alvinEntry = responses.find { it?.entry != null }?.entry?.toEntry()

        when {
            historyEntry != null -> {
                if (historyEntry.getParentId() == null) {
                    mutex.withLock {
                        logger.info("Add missing history entry: ${historyEntry.getId()}")
                        history.addEntry(historyEntry)
                        return true
                    }
                }

                when (isEntryCommitted(historyEntry.getParentId()!!)) {
                    true ->
                        mutex.withLock {
                            logger.info("Add missing history entry: ${historyEntry.getId()}")
                            history.addEntry(historyEntry)
                            return true
                        }

                    false ->
                        return false

                    null ->
                        return null
                }
            }

            alvinEntry != null -> {
                resetFailureDetector(alvinEntry)
                return null
            }

            else ->
                return false
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

    private suspend fun resetFailureDetector(entry: AlvinEntry) = mutex.withLock {
        val entryId = entry.entry.getId()
        entryIdToFailureDetector[entryId]?.cancelCounting()
        entryIdToFailureDetector.putIfAbsent(entryId, getFailureDetectorTimer())
        entryIdToFailureDetector[entryId]!!.startCounting(entry.epoch) {
            recoveryPhase(entry)
        }
    }

    private suspend fun updateEntry(entry: AlvinEntry) = mutex.withLock {
        val entryId = entry.entry.getId()
        entryIdToAlvinEntry[entryId] = entry
        deliveryQueue.removeIf { it.entry == entry.entry }
        deliveryQueue.add(entry)
    }

    private fun peers() = peerResolver.getPeersFromCurrentPeerset()
    private fun getOtherPeers() = peerResolver
        .getPeersFromCurrentPeerset()
        .filter { it.globalPeerId != peerResolver.currentPeer() }

    private suspend fun getNextNum(peerId: Int = globalPeerId.peerId): Int = mutex.withLock {
        val previousMod = lastTransactionId % peers().size
        val removeMod = lastTransactionId - previousMod
        if (peerId > previousMod)
            lastTransactionId = removeMod + peerId
        else
            lastTransactionId = removeMod + peers().size + peerId

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



