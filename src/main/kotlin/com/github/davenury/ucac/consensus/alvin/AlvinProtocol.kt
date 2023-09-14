package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.txblocker.TransactionAcquisition
import com.github.davenury.common.txblocker.TransactionBlocker
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.consensus.SynchronizationMeasurement
import com.zopa.ktor.opentracing.launchTraced
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
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
    private val isMetricTest: Boolean,
    private val subscribers: Subscribers?,
    private val maxChangesPerMessage: Int,
) : AlvinBroadcastProtocol, SignalSubject {
    //  General consesnus
    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()
    private val peerId = peerResolver.currentPeer()
    private val mutex = Mutex()

    //  Alvin specific fields
    private var lastTransactionId = 0
    private val entryIdToAlvinEntry: ConcurrentHashMap<String, AlvinEntry> = ConcurrentHashMap()
    private val entryIdToFailureDetector: ConcurrentHashMap<String, ProtocolTimer> = ConcurrentHashMap()
    private val peerIdToTransactionId: ConcurrentHashMap<PeerId, Int> = ConcurrentHashMap()
    private val initialChangeId = transactionBlocker.getChangeId()

    private val votesContainer = VotesContainer()
    private val synchronizationMeasurement =
        SynchronizationMeasurement(history, protocolClient, this, peerId)

    private var executorService: ExecutorCoroutineDispatcher =
        Executors.newCachedThreadPool().asCoroutineDispatcher()

    private val deliveryQueue: PriorityQueue<AlvinEntry> =
        PriorityQueue { o1, o2 -> o1.transactionId.compareTo(o2.transactionId) }


    override fun getPeerName() = peerId.toString()

    override suspend fun begin() {
        initialChangeId
            ?.let { TransactionAcquisition(ProtocolName.CONSENSUS, it) }
            ?.let { transactionBlocker.tryRelease(it) }
        synchronizationMeasurement.begin(ctx)
        Metrics.bumpLeaderElection(peerResolver.currentPeer(), peersetId)
        subscribers?.notifyAboutConsensusLeaderChange(peerId, peersetId)
    }

    override suspend fun handleProposalPhase(message: AlvinPropose): AlvinAckPropose {
        logger.info("Handle proposal from peer ${message.peerId} for entry: ${message.entry.toEntry().entry.getId()}")
        val change = Change.fromHistoryEntry(message.entry.toEntry().entry)

        signalPublisher.signal(
            Signal.AlvinHandleMessages,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        val newDeps: List<HistoryEntry>
        val newPos: Int
        mutex.withLock {
            updateEntry(message.entry.toEntry())
            resetFailureDetector(message.entry.toEntry())
            val parentEntries = deliveryQueue.map { it.entry.getParentId() }.toSet()
            newDeps = deliveryQueue.map { it.entry }.filter { !parentEntries.contains(it.getId()) }
            newPos = getNextNum(message.peerId)
        }


        signalPublisher.signal(
            Signal.AlvinReceiveProposal,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = Change.fromHistoryEntry(HistoryEntry.deserialize(message.entry.serializedEntry))
        )

        return AlvinAckPropose(newDeps.map { it.serialize() }, newPos)
    }

    override suspend fun handleAcceptPhase(message: AlvinAccept): AlvinAckAccept {
        val entry = message.entry.toEntry()
        val newDeps: List<HistoryEntry>
        val change = Change.fromHistoryEntry(entry.entry)
        val changeId = change!!.id

        logger.info("Handle accept from peer ${message.peerId} for entry: ${entry.entry.getId()}, deps: ${entry.deps.map { it.getId() }}")

        signalPublisher.signal(
            Signal.AlvinHandleMessages,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        checkTransactionBlocker(entry)

        mutex.withLock {
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
        val change = Change.fromHistoryEntry(entry.entry)

        logger.info("Handle stable from peer ${message.peerId} for entry: ${entry.entry.getId()}, deps: ${entry.deps.map { it.getId() }}")

        signalPublisher.signal(
            Signal.AlvinHandleMessages,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        checkTransactionBlocker(entry)

        mutex.withLock {
            updateEntry(entry)
            resetFailureDetector(entry)
        }
        deliverTransaction()


        return AlvinAckStable(peerId)
    }

    override suspend fun handlePrepare(message: AlvinAccept): AlvinPromise {
        val messageEntry = message.entry.toEntry()
        val updatedEntry: AlvinEntry
        val entryId = messageEntry.entry.getId()
        val change = Change.fromHistoryEntry(messageEntry.entry)
        signalPublisher.signal(
            Signal.AlvinHandleMessages,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        logger.info("Handle prepare from peer ${message.peerId} for entry: (${message.peerId},${messageEntry.entry.getId()}), deps: ${messageEntry.deps.map { it.getId() }}")

        if (isTransactionFinished(entryId, change!!.id)) {
            return AlvinPromise(messageEntry.toDto(), true)
        }


        mutex.withLock {
            val entry = entryIdToAlvinEntry[entryId]
            if (entry != null && entry.epoch >= message.entry.epoch) {
                logger.info("Outdated AlvinPrepare message")
                throw AlvinOutdatedPrepareException(
                    message.entry.epoch,
                    entry.epoch
                )
            }
            updatedEntry = entry?.copy(epoch = message.entry.epoch) ?: messageEntry.copy(epoch = message.entry.epoch)
            updateEntry(updatedEntry)
        }

        return AlvinPromise(updatedEntry.toDto(), false)
    }

    override suspend fun handleCommit(message: AlvinCommit): AlvinCommitResponse {
        val messageEntry = message.entry.toEntry()
        val change = Change.fromHistoryEntry(messageEntry.entry)!!
        val entryId = messageEntry.entry.getId()

        mutex.withLock {
            logger.info("Handle commit: ${message.result} from peer: ${message.peerId}, for entry: ${messageEntry.entry.getId()}")
            signalPublisher.signal(
                Signal.AlvinHandleMessages,
                this@AlvinProtocol,
                mapOf(peersetId to otherConsensusPeers()),
                change = change
            )

            if (isTransactionFinished(entryId, change.id)) {
                val entryStatus = getEntryStatus(messageEntry.entry, change.id)
                logger.info("Entry was finished with status: $entryStatus, entryId: $entryId")
                return AlvinCommitResponse(entryStatus, peerId)
            }
        }


        checkTransactionBlocker(messageEntry)
        mutex.withLock {
            changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
            votesContainer.initializeEntry(entryId)
            votesContainer.voteOnEntry(entryId, message.result, message.peerId)

            checkVotes(messageEntry, change)
            checkNextChanges(entryId)

            val entryStatus = getEntryStatus(messageEntry.entry, change.id)

            logger.info("HandleCommit entry was status: $entryStatus, entryId: $entryId")
            return AlvinCommitResponse(entryStatus, peerId)
        }
    }

    override suspend fun handleFastRecovery(message: AlvinFastRecovery): AlvinFastRecoveryResponse {

        logger.info(
            "Handle FastRecovery from peer ${message.peerId.peerId} for entry: ${message.askedEntryId} and isCommitted: ${
                history.containsEntry(
                    message.askedEntryId
                )
            }, currentEntryId: ${message.currentEntryId}"
        )

        val historyEntries = history.getAllEntriesUntilHistoryEntryId(message.currentEntryId)

        logger.info("HandleFastRecovery Entries size: ${historyEntries.size}")

        if (historyEntries.size > maxChangesPerMessage) {
            val limitedEntries = historyEntries.take(maxChangesPerMessage)
            return AlvinFastRecoveryResponse(listOf(), limitedEntries.map { it.serialize() }, false)
        }


        val alvinEntries = mutableListOf<AlvinEntry>()
        var alvinEntryId = message.askedEntryId
        mutex.withLock {
            while (entryIdToAlvinEntry.containsKey(alvinEntryId)) {
                val alvinEntry = entryIdToAlvinEntry[alvinEntryId]!!
                alvinEntries.add(alvinEntry)
                alvinEntryId = alvinEntry.entry.getParentId()!!
            }
        }

        logger.info("HandleFastRecovery Gathered alvinEntries: ${alvinEntries.size}, historyEntries: ${historyEntries.size}")

        if (historyEntries.size + alvinEntries.size > maxChangesPerMessage) {
            val limitedAlvinEntries = alvinEntries.take(maxChangesPerMessage - historyEntries.size)
            return AlvinFastRecoveryResponse(
                limitedAlvinEntries.map { it.toDto() },
                historyEntries.map { it.serialize() },
                false
            )
        }

        return AlvinFastRecoveryResponse(alvinEntries.map { it.toDto() }, historyEntries.map { it.serialize() }, true)
    }


    override suspend fun getProposedChanges(): List<Change> =
        mutex.withLock {
            entryIdToAlvinEntry
                .values
                .filter { it.status != AlvinStatus.STABLE }
                .mapNotNull { Change.fromHistoryEntry(it.entry) }
        }

    override suspend fun getAcceptedChanges(): List<Change> =
        mutex.withLock {
            history
                .toEntryList(true)
                .mapNotNull { Change.fromHistoryEntry(it) }
        }

    override fun amILeader(): Boolean = true

    override fun getLeaderId(): PeerId? = peerResolver.currentPeer()

    override fun getState(): History = history
    override suspend fun isSynchronized(): Boolean = synchronizationMeasurement.isSynchronized()

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]


    override fun otherConsensusPeers(): List<PeerAddress> {
        return peerResolver.getPeersFromPeerset(peersetId).filter { it.peerId != peerId }
    }


    override fun stop() {
        executorService.close()
    }


    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture<ChangeResult>())
        val result = changeIdToCompletableFuture[change.id]!!
        proposeChangeToLedger(result, change)

        return result
    }

    override suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change) {
        val entry = change.toHistoryEntry(peersetId)
        mutex.withLock {
            if (history.containsEntry(entry.getId()) || entryIdToAlvinEntry.contains(entry.getId())) {
                logger.info("Already proposed that change: ${entry.getId()}")
                return
            }

//          FIXME: show up in 1000 Alvin changes

            val isAcquiredTransactionBlocker =
                transactionBlocker.tryAcquireReentrant(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))

            if (!isAcquiredTransactionBlocker) {
                logger.info("Transaction is blocked on protocol ${transactionBlocker.getProtocolName()}, timeout transaction")
                result.complete(ChangeResult(ChangeResult.Status.TIMEOUT))
                return
            }

            val fastRecoveryResult: Boolean? =
                if (!history.isEntryCompatible(entry)) fastRecoveryPhase(entry.getParentId()!!)
                else null

            if (!history.isEntryCompatible(entry)) {
                logger.info(
                    "Proposed change is incompatible. FastRecovery result: $fastRecoveryResult \n CurrentChange: ${
                        history.getCurrentEntry().getId()
                    } \n Change.parentId: ${
                        change.toHistoryEntry(peersetId).getParentId()
                    } \n Change.id: ${change.toHistoryEntry(peersetId).getId()}"
                )
                result.complete(
                    ChangeResult(
                        ChangeResult.Status.CONFLICT,
                        currentEntryId = history.getCurrentEntryId()
                    )
                )
                transactionBlocker.release(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
                return
            }
        }
        logger.info("Propose change to ledger: $change")
        proposalPhase(change)
    }


    private suspend fun proposalPhase(change: Change) {

        val historyEntry = change.toHistoryEntry(peersetId)
        val myDeps = listOf(history.getCurrentEntry())

        logger.info("Starts proposal phase ${historyEntry.getId()}")
        val entry: AlvinEntry
        mutex.withLock {
            entry = AlvinEntry(historyEntry, getNextNum(), myDeps)
            updateEntry(entry)
        }

        val channel = Channel<RequestResult<AlvinAckPropose>>()

        val jobs = scheduleMessages(historyEntry, channel, entry.epoch) { peerAddress ->
            protocolClient.sendProposal(peerAddress, AlvinPropose(peerId, entry.toDto()))
        }
        val responses: List<AlvinAckPropose>? =
            waitForQuorum(historyEntry, jobs, channel, AlvinStatus.PENDING)

        if (responses == null) {
            resetFailureDetector(entry)
            return
        }

        val newPos = responses.maxOf { it.newPos }
        val newDeps = responses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        signalPublisher.signal(
            Signal.AlvinAfterProposalPhase,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        decisionPhase(historyEntry, newPos, (myDeps + newDeps).distinct())
    }

    private suspend fun decisionPhase(historyEntry: HistoryEntry, pos: Int, deps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        val change = Change.fromHistoryEntry(historyEntry)!!
        val changeId = change.id
        entry = entry.copy(transactionId = pos, deps = deps, status = AlvinStatus.ACCEPTED)
        mutex.withLock {
            updateEntry(entry)
        }
        logger.info("Starts decision phase ${entry.entry.getId()}")

        val acceptChannel = Channel<RequestResult<AlvinAckAccept>>()

        val jobs = scheduleMessages(historyEntry, acceptChannel, entry.epoch) { peerAddress ->
            protocolClient.sendAccept(peerAddress, AlvinAccept(peerId, entry.toDto()))
        }

        signalPublisher.signal(
            Signal.AlvinAfterAcceptPhase,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )

        val newResponses = waitForQuorum(historyEntry, jobs, acceptChannel, AlvinStatus.PENDING)

        if (newResponses == null) {
            resetFailureDetector(entry)
            return
        }

        val newDeps = newResponses.flatMap { it.newDeps }.map { HistoryEntry.deserialize(it) }

        deliveryPhase(historyEntry, (deps + newDeps).distinct())
    }

    private suspend fun deliveryPhase(historyEntry: HistoryEntry, newDeps: List<HistoryEntry>) {
        var entry = entryIdToAlvinEntry[historyEntry.getId()]!!
        val change = Change.fromHistoryEntry(historyEntry)!!
        val changeId = change.id
        entry = entry.copy(deps = newDeps, status = AlvinStatus.STABLE)
        mutex.withLock {
            updateEntry(entry)
        }

        logger.info("Starts delivery phase ${entry.entry.getId()}")

//      FIXME: Maybe instead of sending last stable we will send last 5?

        mutex.withLock {
            otherConsensusPeers().forEach {
//                if (entry.transactionId > (peerIdToTransactionId[it.peerId] ?: -1))
                peerIdToTransactionId[it.peerId] = entry.transactionId
//                else {
//                    logger.info("Not setting transactionId for peer: ${it.peerId}, current transactionId: ${peerIdToTransactionId[it.peerId]}, entry transactionId: ${entry.transactionId}")
//                }
            }
        }


        scheduleMessages(historyEntry, null, entry.epoch) { peerAddress ->
            if ((peerIdToTransactionId[peerAddress.peerId] ?: 0) == entry.transactionId)
                protocolClient.sendStable(peerAddress, AlvinStable(peerId, entry.toDto()))
            else {
//              Dummy response to stop repeat stable
                logger.info("Stop sending stable to peer: ${peerAddress.peerId} for entry: ${entry.entry.getId()}, peer transactionId: ${entry.transactionId}")
                ConsensusResponse(peerAddress.address, AlvinAckStable(peerAddress.peerId))
            }
        }

        signalPublisher.signal(
            Signal.AlvinAfterStablePhase,
            this@AlvinProtocol,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )
        deliverTransaction()
    }

    private suspend fun recoveryPhase(entry: AlvinEntry) {

        logger.info("Starts recovery phase for ${entry.entry.getId()}")

        checkTransactionBlocker(entry)

        var newEntry = entry.copy(epoch = entry.epoch + 1, status = AlvinStatus.UNKNOWN)
        mutex.withLock {
            updateEntry(newEntry)
        }
        val changeId = Change.fromHistoryEntry(entry.entry)!!.id

        val promiseChannel = Channel<RequestResult<AlvinPromise>>()

        val jobs = scheduleMessages(entry.entry, promiseChannel, newEntry.epoch) { peerAddress ->
            protocolClient.sendPrepare(
                peerAddress,
                AlvinAccept(peerId, newEntry.toDto())
            )
        }

        val responses = waitForQuorum(entry.entry, jobs, promiseChannel, AlvinStatus.UNKNOWN, includeMyself = false)

        if (responses == null) {
            resetFailureDetector(entry)
            return
        }

        val newDeps = responses
            .filter { it.entry != null }
            .flatMap { it.entry!!.deps }
            .distinct()
        val newPos =
            responses.filter { it.entry != null }.maxOfOrNull { it.entry!!.transactionId } ?: entry.transactionId

        newEntry = newEntry.copy(transactionId = newPos, deps = newDeps.map { HistoryEntry.deserialize(it) })

        when {
            responses.any { it.isFinished } -> mutex.withLock {
                if (!fastRecoveryPhase(entry.entry.getId())) {
                    logger.error("Some state is inconsistent entryId: ${entry.entry.getId()}")
                }
            }


            responses.any { it.entry?.status == AlvinStatus.STABLE } -> {
                newEntry = newEntry.copy(status = AlvinStatus.STABLE)
                mutex.withLock {
                    updateEntry(newEntry)
                }
                deliveryPhase(newEntry.entry, newEntry.deps)
            }

            responses.any { it.entry?.status == AlvinStatus.ACCEPTED } -> {
                newEntry = newEntry.copy(status = AlvinStatus.ACCEPTED)
                mutex.withLock {
                    updateEntry(newEntry)
                }
                decisionPhase(newEntry.entry, newEntry.transactionId, newEntry.deps)
            }

            else -> {
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
        val transactionAcquisition =
            TransactionAcquisition(ProtocolName.CONSENSUS, Change.fromHistoryEntry(entry.entry)!!.id)

        if (isTransactionFinished(entry.entry.getId(), changeId)) return

        val isTransactionBlockerAcquired = transactionBlocker.tryAcquireReentrant(transactionAcquisition)


        when {
            !isTransactionBlockerAcquired && transactionBlocker.getProtocolName() != ProtocolName.CONSENSUS -> {
                throw AlvinHistoryBlockedException(
                    transactionBlocker.getChangeId()!!,
                    transactionBlocker.getProtocolName()!!
                )
            }

            !isTransactionBlockerAcquired -> {
                val changeId = transactionBlocker.getChangeId()

                val alvinEntry = entryIdToAlvinEntry
                    .values
                    .firstOrNull { Change.fromHistoryEntry(it.entry)?.id == changeId }

                if (alvinEntry == null) {
                    logger.info("Release transaction blocker because doesn't have change with Id $changeId")
                    transactionBlocker.tryRelease(
                        TransactionAcquisition(
                            ProtocolName.CONSENSUS,
                            changeId!!
                        )
                    )
                    checkTransactionBlocker(entry)
                    return
                }

                logger.info("Try to fast recovery from being blocked on entryId: ${alvinEntry.entry.getId()}")

                val resultRecovery: Boolean

                if (transactionBlocker.getChangeId() == changeId) mutex.withLock {
                    resultRecovery = fastRecoveryPhase(alvinEntry.entry.getId())
                }
                else {
                    return checkTransactionBlocker(entry)
                }
                if (resultRecovery) {
                    logger.info("Entry: ${entry.entry.getId()} was aborted, when I was inactive")
                    transactionBlocker.tryRelease(
                        TransactionAcquisition(
                            ProtocolName.CONSENSUS,
                            changeId!!
                        )
                    )
                    checkTransactionBlocker(entry)
                } else throw AlvinHistoryBlockedException(
                    transactionBlocker.getChangeId()!!,
                    transactionBlocker.getProtocolName()!!
                )
            }

            else -> {
                logger.info("Transaction blocker acquired for entry: $changeId")
            }
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
                launchTraced(MDCContext()) {
                    var peerAddress: PeerAddress
                    var response: ConsensusResponse<A?>
                    do {
                        peerAddress = otherConsensusPeers()[it]
                        response = sendMessage(peerAddress)

                        val entry: AlvinEntry?
                        mutex.withLock {
                            entry = entryIdToAlvinEntry[entryId]
                        }

//                      FIXME: These returns leads to infinity waiting in waitforQurum messages

                        if (entry != null && entry.epoch > epoch) {
                            logger.info("Our entry epoch increased so stop sending message, scheduleMessage epoch ${epoch}, new epoch: ${entry.epoch}, entryId: ${entryId}")
                            mutex.withLock {
                                resetFailureDetector(entry)
                            }
                            channel?.send(
                                RequestResult(
                                    entryId,
                                    response.message,
                                    response.unauthorized,
                                    isInterrupted = true
                                )
                            )
                            return@launchTraced
                        }

                        if (epoch != -1 && entry != null && response.unauthorized) {
                            logger.debug("Peer responded that our epoch is outdated, wait for newer messages")
                            mutex.withLock {
                                resetFailureDetector(entry)
                            }
                            channel?.send(
                                RequestResult(
                                    entryId,
                                    response.message,
                                    response.unauthorized,
                                    isInterrupted = true
                                )
                            )
                            return@launchTraced
                        }

                        if (response.message == null) {
                            logger.info("Delay message to peer ${peerAddress.peerId} for ${heartbeatDelay.toMillis()} ms")
                            delay(heartbeatDelay.toMillis())
                        }

                    } while (response.message == null)

                    channel?.send(
                        RequestResult(
                            entryId,
                            response.message!!,
                            response.unauthorized
                        )
                    )
                }
            }
        }

    private suspend fun <A> scheduleMessagesOnce(
        entryId: String,
        channel: Channel<RequestResult<A?>>?,
        sendMessage: suspend (peerAddress: PeerAddress) -> ConsensusResponse<A?>
    ) =
        (0 until otherConsensusPeers().size).map {
            with(CoroutineScope(executorService)) {
                launchTraced(MDCContext()) {
                    val peerAddress = otherConsensusPeers()[it]
                    val response = sendMessage(peerAddress)
                    channel?.send(RequestResult(entryId, response.message))
                }
            }
        }

    private suspend fun <A> waitForQuorum(
        historyEntry: HistoryEntry,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus,
        includeMyself: Boolean = true
    ): List<A>? = waitForQuorum<A>(historyEntry.getId(), jobs, channel, status, includeMyself)

    private suspend fun <A> waitForQuorum(
        entryId: String,
        jobs: List<Job>,
        channel: Channel<RequestResult<A>>,
        status: AlvinStatus,
        includeMyself: Boolean = true
    ): List<A>? {
        val responses: MutableList<A> = mutableListOf()
        val changeSize = if (includeMyself) 0 else -1
        while (!isMoreThanHalf(responses.size + changeSize)) {
            val response = channel.receive()
            val entry = entryIdToAlvinEntry[entryId]

            when {
                response.isInterrupted -> {
                    return null
                }

                response.entryId == entryId && !response.unauthorized -> responses.add(response.response!!)
                response.entryId == entryId -> mutex.withLock {
                    resetFailureDetector(entry!!)
                    throw AlvinLeaderBecameOutdatedException(Change.fromHistoryEntry(entry.entry)!!.id)
                }

                status == entry?.status && status != AlvinStatus.UNKNOWN -> channel.send(response)
            }
        }

        jobs.forEach { if (it.isActive) it.cancel() }

        return responses
    }

    private suspend fun <A> gatherResponses(
        entryId: String,
        channel: Channel<RequestResult<A>>,
    ): List<A?>? {
        val responses: MutableList<A?> = mutableListOf()
        while (responses.size < otherConsensusPeers().size) {
            val response = channel.receive()
            if (response.isInterrupted) return null
            else if (response.entryId == entryId) responses.add(response.response)
            else if (!history.containsEntry(entryId)) channel.send(response)
        }

        return responses
    }


    private suspend fun isBlockedOnDifferentProtocol() =
        transactionBlocker.isAcquired() && transactionBlocker.getProtocolName() != ProtocolName.CONSENSUS

    //  TODO: Check if any transaction from delivery queue can be commited or aborted
//  If can be committed send commit message to all other peers and wait for qurom commit messages.
    private suspend fun deliverTransaction(): Unit {

        var resultEntry: AlvinEntry
        do {
            if (deliveryQueue.size == 0) {
                return
            }
            val entry = cleanDeps(deliveryQueue.poll())
            val changeId = Change.fromHistoryEntry(entry.entry)!!.id
            resultEntry = entry
            if (history.containsEntry(entry.entry.getId())) {
                scheduleCommitMessagesOnce(entry, AlvinResult.COMMIT)
            } else if (changeIdToCompletableFuture[changeId]?.isDone == true) {
                scheduleCommitMessagesOnce(entry, AlvinResult.ABORT)
            }
        } while (changeIdToCompletableFuture[changeId]?.isDone == true)


        val entry: AlvinEntry = resultEntry

        mutex.withLock {
            if (entry.status != AlvinStatus.STABLE) {
                logger.info("Entry is not stable yet (id: ${entry.entry.getId()}")
                deliveryQueue.add(entry)
                return
            }
        }

        val associatedDeps = entry.deps.associate {
            it.getId() to isEntryFinished(it.getId())
        }

        val depsResult = associatedDeps.map { it.value }

        when {
            depsResult.all { it } && !history.isEntryCompatible(entry.entry) -> {
                logger.info("Entry ${entry.entry.getId()} is incompatible, send abort, deps: $associatedDeps")
                scheduleCommitMessagesOnce(entry, AlvinResult.ABORT)
                mutex.withLock {
                    resetFailureDetector(entry) {
                        failureDetectorScheduleRepeat(entry, AlvinResult.ABORT)
                    }
                }
            }

            depsResult.all { it } -> {
                logger.info("Entry ${entry.entry.getId()} is compatible, send commit")
                scheduleCommitMessagesOnce(entry, AlvinResult.COMMIT)
                mutex.withLock {
                    resetFailureDetector(entry) {
                        failureDetectorScheduleRepeat(entry, AlvinResult.COMMIT)
                    }
                }
            }

            else -> mutex.withLock {
                deliveryQueue.add(entry)
                resetFailureDetector(entry) {
                    deliverTransaction()
                }
            }
        }
    }

    private suspend fun failureDetectorScheduleRepeat(entry: AlvinEntry, result: AlvinResult) {
        val changeId: String = Change.fromHistoryEntry(entry.entry)!!.id
        if (changeIdToCompletableFuture[changeId]?.isDone == true) return
        logger.info("Send result again: $result for entry: ${entry.entry.getId()}")
        scheduleCommitMessagesOnce(entry, result)

        mutex.withLock {
            resetFailureDetector(entry) {
                failureDetectorScheduleRepeat(entry, result)
            }
        }
    }

    private suspend fun scheduleCommitMessagesOnce(
        entry: AlvinEntry,
        result: AlvinResult,
    ) {
        scheduleMessagesOnce(entry.entry.getId(), null) { peerAddress ->
            val response = protocolClient.sendCommit(peerAddress, AlvinCommit(entry.toDto(), result, peerId))
            if (response.message != null) updateVotes(entry, response.message)
            response
        }
    }

    private suspend fun updateVotes(entry: AlvinEntry, response: AlvinCommitResponse): Unit = mutex.withLock {
        val change = Change.fromHistoryEntry(entry.entry)!!
        val entryId = entry.entry.getId()

        if (changeIdToCompletableFuture[change.id]?.isDone == true) return
        if (response.result == null) return@withLock

        votesContainer.voteOnEntry(entryId, response.result, response.peerId)
        checkVotes(entry, change)
        checkNextChanges(entryId)
    }


    //  Mutex function
    private suspend fun checkVotes(entry: AlvinEntry, change: Change) {
        val entryId = entry.entry.getId()
        val myselfVotesForCommit = history.isEntryCompatible(entry.entry)

        val (commitVotes, abortVotes) = votesContainer.getVotes(entryId, myselfVotesForCommit)

        val (commitDecision, abortDecision) = Pair(isMoreThanHalf(commitVotes), isMoreThanHalf(abortVotes))

        logger.info("Votes for committing: $commitVotes, aborting: $abortVotes, for entryId: $entryId")

        if ((commitDecision || abortDecision) && !isTransactionFinished(entryId, change.id)) {
            if (commitDecision && !history.containsEntry(entry.entry.getParentId()!!)) {
                logger.info("Waiting until parent will be processed ${entry.entry.getParentId()} \n CommitDecision: $commitDecision, abortDecision: $abortDecision")
                return
            }
            cleanAfterEntryFinished(entryId)

            val changeResult = if (commitDecision) {
                commitChange(entry.entry)
                ChangeResult.Status.SUCCESS
            } else {
                abortChange(entry.entry)
                ChangeResult.Status.CONFLICT
            }
            logger.info("The result of change (${change.id}) is $changeResult")
            transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change.id))
        }
    }

    private suspend fun checkNextChanges(entryId: String) {
        var currentEntryId = entryId
        do {
            val entry =
                entryIdToAlvinEntry.toList().firstOrNull { it.second.entry.getParentId() == currentEntryId }?.second
            if (entry != null) {
                val change = Change.fromHistoryEntry(entry.entry)!!
                checkVotes(entry, change)
                currentEntryId = entry.entry.getId()
            }
        } while (entry != null)
    }

    //  FIXME: we should only wait until all deps are finished not necessarily committed
    private suspend fun isEntryFinished(entryId: String): Boolean = mutex.withLock {
        if (history.containsEntry(entryId)) return true
        if (entryIdToAlvinEntry[entryId] != null) {
            resetFailureDetector(entryIdToAlvinEntry[entryId]!!)
            return false
        }

        return fastRecoveryPhase(entryId)
    }


    private suspend fun fastRecoveryPhase(entryId: String): Boolean {
        logger.info("Start fast recovery for entryId: $entryId")

        var responses: List<AlvinFastRecoveryResponse>?

        do {
            val fastRecoveryChannel = Channel<RequestResult<AlvinFastRecoveryResponse?>>()

            logger.info("Send fast recovery with id: ${history.getCurrentEntryId()}")

//      epoch 1 because we don't current value
            scheduleMessagesOnce(entryId, fastRecoveryChannel) {
                protocolClient.sendFastRecovery(it, AlvinFastRecovery(entryId, history.getCurrentEntryId(), peerId))
            }

            responses = gatherResponses(entryId, fastRecoveryChannel)?.filterNotNull()

            responses
                ?.flatMap { it.historyEntries.map { HistoryEntry.deserialize(it) } }
                ?.distinct()
                ?.forEach {
                    val change = Change.fromHistoryEntry(it)
                    commitChange(it)
                    cleanAfterEntryFinished(it.getId())
                    transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, change!!.id))
                }
        } while (responses == null || !responses.all { it.isFinished })


        val findEntry = { response: AlvinFastRecoveryResponse ->
            response.entries.find { it?.toEntry()?.entry?.getId() == entryId }?.toEntry()
        }

        val alvinEntry = responses
            .find { findEntry(it) != null }
            ?.let { findEntry(it) }


        return when {
            history.containsEntry(entryId) -> true

            alvinEntry != null -> {
                resetFailureDetector(alvinEntry)
                false
            }

            else -> true
        }
    }


    //  mutex function
    private suspend fun commitChange(historyEntry: HistoryEntry) {
        if (!history.containsEntry(historyEntry.getId())) {
            history.addEntry(historyEntry)
            synchronizationMeasurement.entryIdCommitted(historyEntry.getId(), Instant.now())
        }
        val change = Change.fromHistoryEntry(historyEntry)!!
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        if (isMetricTest) {
            Metrics.bumpChangeMetric(
                changeId = change.id,
                peerId = peerId,
                peersetId = peersetId,
                protocolName = ProtocolName.CONSENSUS,
                state = "accepted"
            )
        }
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.SUCCESS))
        signalPublisher.signal(
            Signal.AlvinCommitChange,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )
    }

    private fun abortChange(historyEntry: HistoryEntry) {
        val change = Change.fromHistoryEntry(historyEntry)!!
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        changeIdToCompletableFuture[change.id]!!.complete(
            ChangeResult(
                ChangeResult.Status.CONFLICT,
                currentEntryId = history.getCurrentEntryId()
            )
        )
        signalPublisher.signal(
            Signal.AlvinAbortChange,
            this,
            mapOf(peersetId to otherConsensusPeers()),
            change = change
        )
    }

    private suspend fun cleanAfterEntryFinished(entryId: String) {
        entryIdToAlvinEntry.remove(entryId)
        entryIdToFailureDetector[entryId]?.cancelCounting()
        entryIdToFailureDetector.remove(entryId)
        votesContainer.removeEntry(entryId)
    }


    private fun cleanDeps(entry: AlvinEntry): AlvinEntry {
        val finalDeps = entry.deps.filter {
            val alvinEntry = entryIdToAlvinEntry[it.getId()]
            alvinEntry != null && alvinEntry.transactionId < entry.transactionId
        }

        val unknownDeps =
            entry.deps.filter { !history.containsEntry(it.getId()) && it.getId() != entry.entry.getId() }

        return entry.copy(deps = unknownDeps + finalDeps)
    }

    //  mutex function
    private suspend fun resetFailureDetector(entry: AlvinEntry) {
        resetFailureDetector(entry) {
            try {
                val change = Change.fromHistoryEntry(entry.entry)!!
                if (changeIdToCompletableFuture[change.id]?.isDone != false)
                    recoveryPhase(entry)
            } catch (ex: Exception) {
                logger.error("Exception was thrown during recovery", ex)
            }
        }
    }

    //  mutex function
    private suspend fun resetFailureDetector(entry: AlvinEntry, function: suspend () -> Unit) {
        val entryId = entry.entry.getId()
        entryIdToFailureDetector[entryId]?.cancelCounting()
        entryIdToFailureDetector.putIfAbsent(entryId, getFailureDetectorTimer())
        if (entryIdToAlvinEntry.containsKey(entryId)) entryIdToFailureDetector[entryId]!!.startCounting(
            entry.epoch,
            function
        )
    }

    //  Mutex function
    private fun updateEntry(entry: AlvinEntry) {
        val entryId = entry.entry.getId()
        val oldEntry = entryIdToAlvinEntry[entryId]
        val changeId: String = Change.fromHistoryEntry(entry.entry)!!.id

        if (isTransactionFinished(entryId, changeId)) return

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


    //  Mutex function
    private suspend fun getNextNum(peerId: PeerId = this.peerId): Int {
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
    private fun getFastRecoveryTimer() = ProtocolTimerImpl(heartbeatDelay, heartbeatDelay.dividedBy(2), ctx)

    private fun isTransactionFinished(entryId: String, changeId: String) =
        history.containsEntry(entryId) || changeIdToCompletableFuture[changeId]?.isDone == true

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
    val response: A?,
    val unauthorized: Boolean = false,
    val isInterrupted: Boolean = false,
)


enum class AlvinStatus {
    PENDING, ACCEPTED, STABLE, UNKNOWN
}