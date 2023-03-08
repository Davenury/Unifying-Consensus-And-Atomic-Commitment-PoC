package com.github.davenury.ucac.consensus.alvin

import AlvinPropose
import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

class AlvinProtocol(
    private val history: History,
    private var peerAddress: String,
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
    private val entryIdToAlvinEntry: MutableMap<String, AlvinEntry> = mutableMapOf()
    private var executorService: ExecutorCoroutineDispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()


    override fun getPeerName() = globalPeerId.toString()

    override suspend fun begin() {
        TODO("Not yet implemented")
    }

    override suspend fun handleProposalPhase(change: Change): CompletableFuture<ChangeResult> {
        TODO("Not yet implemented")
    }

    override suspend fun handleDecisionPhase(change: Change): CompletableFuture<ChangeResult> {
        TODO("Not yet implemented")
    }

    override suspend fun handleAcceptPhase(change: Change): CompletableFuture<ChangeResult> {
        TODO("Not yet implemented")
    }

    override suspend fun handleDeliveryPhase(change: Change): CompletableFuture<ChangeResult> {
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
        entryIdToAlvinEntry[historyEntry.getId()] = entry

        peers.map {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    protocolClient.sendProposal(it, AlvinPropose(globalPeerId.peerId, entry))
                }
            }
        }

    }

    private suspend fun decisionPhase()
    private suspend fun acceptPhase()
    private suspend fun deliveryPhase()

    private suspend fun getNextNum(): Int = mutex.withLock {
        lastTransactionId = lastTransactionId % peers.size + peers.size + globalPeerId.peerId
        return lastTransactionId
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
)

enum class AlvinStatus {
    PENDING, ACCEPTED, STABLE
}



