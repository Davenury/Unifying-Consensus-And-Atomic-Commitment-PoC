package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
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
    private val peers = peerResolver.getPeersFromCurrentPeerset()
    private var lastTransactionId = 0
    private val entryIdToAlvinEntry: ConcurrentHashMap<String, AlvinEntry> = ConcurrentHashMap()
    private val entryIdToFailureDetector: ConcurrentHashMap<String, ProtocolTimer> = ConcurrentHashMap()
    private var executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()
    private val deliveryQueue: PriorityQueue<AlvinEntry> =
        PriorityQueue { o1, o2 -> o1.transactionId.compareTo(o2.transactionId) }
    private val proposeChannel = Channel<Pair<HistoryEntry, AlvinAckPropose>>()
    private val acceptChannel = Channel<Pair<HistoryEntry, AlvinAckAccept>>()
    private val promiseChannel = Channel<Pair<HistoryEntry, AlvinPromise>>()

    override fun getPeerName() = globalPeerId.toString()

    override suspend fun begin() {
        TODO("Not yet implemented")
    }

//  TODO: use transactionBlocker

    override suspend fun handleProposalPhase(message: AlvinPropose): AlvinAckPropose {
        val newDeps: List<HistoryEntry>
        resetFailureDetector(message.entry)
        updateEntry(message.entry)
        mutex.withLock {
            newDeps = deliveryQueue.map { it.entry }
        }
        val newPos = getNextNum(message.peerId)

        return AlvinAckPropose(newDeps, newPos)
    }

    override suspend fun handleAcceptPhase(message: AlvinAccept): AlvinAckAccept {
        resetFailureDetector(message.entry)
        updateEntry(message.entry)
        val newDeps: List<HistoryEntry>
        mutex.withLock {
            newDeps = entryIdToAlvinEntry
                .values
                .filter { it.transactionId < message.entry.transactionId }
                .map { it.entry }
        }

        return AlvinAckAccept((newDeps + message.entry.deps).distinct(), message.entry.transactionId)
    }

    override suspend fun handleStable(message: AlvinStable): AlvinAckStable {
        resetFailureDetector(message.entry)
        updateEntry(message.entry)
        return AlvinAckStable(globalPeerId.peerId)
    }

    override suspend fun handlePrepare(message: AlvinAccept): AlvinPromise {

        return AlvinPromise()
        TODO("Not yet implemented")
    }

    override suspend fun handleCommit(message: AlvinCommit): AlvinPromise {
        TODO("Not yet implemented")
    }


    override suspend fun getProposedChanges(): List<Change> {
        TODO("Not yet implemented")
    }

    override suspend fun getAcceptedChanges(): List<Change> {
        TODO("Not yet implemented")
    }

    override fun getState(): History = history

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override fun otherConsensusPeers(): List<PeerAddress> {
        return peerResolver.getPeersFromCurrentPeerset().filter { it.globalPeerId != globalPeerId }
    }


    override fun stop() {
        TODO("Not yet implemented")
    }

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

            logger.info("Propose change to ledger: $change")

            with(CoroutineScope(ctx)) {
                launch(MDCContext()) {
                    proposalPhase(change)
                }
            }
        }
    }


    private suspend fun proposalPhase(change: Change) {
        val historyEntry = change.toHistoryEntry(globalPeerId.peersetId)
        val entry = AlvinEntry(historyEntry, getNextNum(), listOf(history.getCurrentEntry()))
        updateEntry(entry)

        val jobs = scheduleMessages(entry, proposeChannel) { peerAddress, entry ->
            protocolClient.sendProposal(peerAddress, AlvinPropose(globalPeerId.peerId, entry))
        }
        val responses: List<AlvinAckPropose> = waitForQurom(historyEntry, jobs, proposeChannel, AlvinStatus.PENDING)

        val newPos = responses.maxOf { it.newPos }
        val newDeps = responses.flatMap { it.newDeps }.distinct()

        decisionPhase(historyEntry, newPos, newDeps)
    }

    private suspend fun decisionPhase(historyEntry: HistoryEntry, pos: Int, deps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(transactionId = pos, deps = deps, status = AlvinStatus.ACCEPTED)
        updateEntry(entry)

        val jobs = scheduleMessages(entry, acceptChannel) { peerAddress, entry ->
            protocolClient.sendAccept(peerAddress, AlvinAccept(globalPeerId.peerId, entry))
        }

        val newResponses = waitForQurom(historyEntry, jobs, acceptChannel, AlvinStatus.PENDING)

        val newDeps = newResponses.flatMap { it.newDeps }.distinct()

        deliveryPhase(historyEntry, newDeps)
    }

    private suspend fun deliveryPhase(historyEntry: HistoryEntry, newDeps: List<HistoryEntry>) {

        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        entry = entry.copy(deps = newDeps, status = AlvinStatus.STABLE)
        updateEntry(entry)

        scheduleMessages(entry, null) { peerAddress, entry ->
            protocolClient.sendStable(peerAddress, AlvinStable(globalPeerId.peerId, entry))
        }
        deliverTransaction(entry)
    }

    private suspend fun recoveryPhase(entry: AlvinEntry) {

        var newEntry = entry.copy(epoch = entry.epoch + 1, status = AlvinStatus.UNKNOWN)
        val entryId = entry.entry.getId()
        entryIdToAlvinEntry[entryId] = entry

        val jobs = scheduleMessages(newEntry, promiseChannel) { peerAddress, entry ->
            protocolClient.sendPrepare(
                peerAddress,
                AlvinAccept(globalPeerId.peerId, entry)
            )
        }

        val responses = waitForQurom(entry.entry, jobs, promiseChannel, AlvinStatus.UNKNOWN)

        val newDeps = responses.flatMap { it.entry.deps }.distinct()
        val newPos = responses.maxOf { it.entry.transactionId }

        newEntry = newEntry.copy(transactionId = newPos, deps = newDeps)

        when {
            responses.any { it.entry.status == AlvinStatus.STABLE } -> {
                newEntry = newEntry.copy(status = AlvinStatus.STABLE)
                updateEntry(newEntry)
                deliveryPhase(newEntry.entry, newEntry.deps)
            }

            responses.any { it.entry.status == AlvinStatus.ACCEPTED } -> {
                newEntry = newEntry.copy(status = AlvinStatus.ACCEPTED)
                updateEntry(newEntry)
                decisionPhase(newEntry.entry, newEntry.transactionId, newEntry.deps)
            }

            else -> {
                newEntry = newEntry.copy(status = AlvinStatus.PENDING)
                updateEntry(newEntry)
                proposalPhase(Change.fromHistoryEntry(newEntry.entry)!!)
            }
        }

    }

    private suspend fun <A> scheduleMessages(
        entry: AlvinEntry,
        channel: Channel<Pair<HistoryEntry, A>>?,
        sendMessage: suspend (peerAddress: PeerAddress, entry: AlvinEntry) -> ConsensusResponse<A?>
    ) =
        peers.map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    var response: ConsensusResponse<A?> = ConsensusResponse(it.address, null)
                    while (response.message == null) {
                        response = sendMessage(it, entry)
                    }
                    channel?.send(Pair(entry.entry, response.message!!))
                }
            }
        }

    private suspend fun <A> waitForQurom(
        historyEntry: HistoryEntry,
        jobs: List<Job>,
        channel: Channel<Pair<HistoryEntry, A>>,
        status: AlvinStatus
    ): List<A> {
        val responses: MutableList<A> = mutableListOf()
        while (!isMoreThanHalf(responses.size)) {
            val response = channel.receive()
            val entry = entryIdToAlvinEntry[historyEntry.getId()]
            if (response.first == historyEntry) responses.add(response.second)
            else if (status == entry?.status) channel.send(response)
        }

        jobs.forEach { if (it.isActive) it.cancel() }

        return responses
    }

    //  TODO: Check if any transaction from delivery queue can be commited or aborted
//  If can be committed send commit message to all other peers and wait for qurom commit messages.
    private fun deliverTransaction(entry: AlvinEntry) {

        deliveryQueue.poll()

        val change = Change.fromHistoryEntry(entry.entry)!!
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())

        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.SUCCESS))
    }

    private suspend fun resetFailureDetector(entry: AlvinEntry) = mutex.withLock {
        val entryId = entry.entry.getId()
        entryIdToFailureDetector[entryId]?.cancelCounting()
        entryIdToFailureDetector.putIfAbsent(entryId, getFailureDetectorTimer())
        entryIdToFailureDetector[entryId]!!.startCounting {
            recoveryPhase(entry)
        }
    }

    private suspend fun updateEntry(entry: AlvinEntry) = mutex.withLock {
        val entryId = entry.entry.getId()
        entryIdToAlvinEntry[entryId] = entry
        deliveryQueue.removeIf { it.entry == entry.entry }
        deliveryQueue.add(entry)
    }

    private suspend fun getNextNum(peerId: Int = globalPeerId.peerId): Int = mutex.withLock {
        val previousMod = lastTransactionId % peers.size
        val removeMod = lastTransactionId - previousMod
        if (peerId > previousMod)
            lastTransactionId = removeMod + peerId
        else
            lastTransactionId = removeMod + peers.size + peerId

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
)

enum class AlvinStatus {
    PENDING, ACCEPTED, STABLE, UNKNOWN
}



