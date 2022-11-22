package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.common.history.IntermediateHistoryEntry
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.math.max

interface GPACProtocol : SignalSubject, AtomicCommitmentProtocol {
    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    suspend fun handleElect(message: ElectMe): ElectedYou
    suspend fun handleAgree(message: Agree): Agreed
    suspend fun handleApply(message: Apply)

    suspend fun performProtocolAsLeader(change: Change, iteration: Int = 1)
    suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int = 1)
    fun getTransaction(): Transaction
    fun getBallotNumber(): Int

}


class GPACProtocolImpl(
    private val history: History,
    private val gpacConfig: GpacConfig,
    private val ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: GPACProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val peerResolver: PeerResolver,
) : GPACProtocol {
    var leaderTimer: ProtocolTimer = ProtocolTimerImpl(gpacConfig.leaderFailDelay, Duration.ZERO, ctx)
    var retriesTimer: ProtocolTimer =
        ProtocolTimerImpl(gpacConfig.initialRetriesDelay, gpacConfig.retriesBackoffTimeout, ctx)
    private val maxLeaderElectionTries = gpacConfig.maxLeaderElectionTries

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()

        val myAddress = peerResolver.currentPeerAddress().address
        val enrichedChange =
            if (change.peers.contains(myAddress)) {
                change
            } else {
                change.withAddress(myAddress)
            }
        changeIdToCompletableFuture[enrichedChange.toHistoryEntry().getId()] = cf

        GlobalScope.launch(MDCContext()) {
            performProtocolAsLeader(enrichedChange)
        }

        return cf
    }

    override suspend fun handleElect(message: ElectMe): ElectedYou {
        val decision = message.acceptNum?.let { acceptNum ->
            Changes.fromHistory(history).find { it.acceptNum == acceptNum }
        }
        if (decision != null) {
            // meaning that I'm the cohort that got apply for transaction of original leader
            return ElectedYou(
                message.ballotNumber,
                Accept.COMMIT,
                message.acceptNum,
                Accept.COMMIT,
                true
            )
        }

        signal(Signal.OnHandlingElectBegin, null, message.change)

        if (transactionBlocker.isAcquired()) {
            logger.info("Tried to respond to elect me when semaphore acquired!")
            throw AlreadyLockedException()
        }

        if (!checkBallotNumber(message.ballotNumber)) {
            throw NotElectingYou(myBallotNumber, message.ballotNumber)
        }

        val initVal =
            if (!shouldCheckForCompatibility(message.change.peers) || history.isEntryCompatible(message.change.toHistoryEntry())) Accept.COMMIT else Accept.ABORT

        transaction = Transaction(ballotNumber = message.ballotNumber, initVal = initVal, change = message.change)

        signal(Signal.OnHandlingElectEnd, transaction, message.change)

        return ElectedYou(
            message.ballotNumber,
            initVal,
            transaction.acceptNum,
            transaction.acceptVal,
            transaction.decision
        )
    }

    override suspend fun handleAgree(message: Agree): Agreed {

        signal(Signal.OnHandlingAgreeBegin, transaction, message.change)

        if (!checkBallotNumber(message.ballotNumber)) {
            throw NotValidLeader(myBallotNumber, message.ballotNumber)
        }
        logger.info("Handling agree $message")
        this.transaction =
            this.transaction.copy(
                ballotNumber = message.ballotNumber,
                acceptVal = message.acceptVal,
                acceptNum = message.acceptNum ?: message.ballotNumber
            )

        logger.info("State transaction state: ${this.transaction}")

        myBallotNumber = message.ballotNumber

        if (!message.decision) {
            transactionBlocker.tryToBlock()
            logger.info("Lock aquired: ${message.ballotNumber}")
        }

        signal(Signal.OnHandlingAgreeEnd, transaction, message.change)

        leaderFailTimeoutStart(message.change)

        return Agreed(transaction.ballotNumber, message.acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        logger.info("HandleApply message: $message")
        val isCurrentTransaction = message.ballotNumber == this.myBallotNumber

        if (isCurrentTransaction) leaderFailTimeoutStop()
        signal(Signal.OnHandlingApplyBegin, transaction, message.change)

        try {
            if (isCurrentTransaction) {
                this.transaction =
                    this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)
            }

            if (message.acceptVal == Accept.COMMIT) {
                signal(Signal.OnHandlingApplyCommitted, transaction, message.change)
            }
            if (message.acceptVal == Accept.COMMIT && !transactionWasAppliedBefore()) {
                addChangeToHistory(message.change)
            }
            changeSucceeded(message.change)
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)

            transactionBlocker.releaseBlock()

            signal(Signal.OnHandlingApplyEnd, transaction, message.change)
        }
    }

    private fun addChangeToHistory(change: Change) {
        change.toHistoryEntry().let {
            if (shouldCheckForCompatibility(change.peers)) {
                it
            } else {
                IntermediateHistoryEntry(it.getContent(), history.getCurrentEntry().getId())
            }
        }.let {
            history.addEntry(it)
        }
    }

    // This function determines if we should check for HistoryEntry compability
    // TODO - change its implementation to one based on peersetsIds when change has peersetId
    private fun shouldCheckForCompatibility(peers: List<String>): Boolean = peers.size == 1

    private fun transactionWasAppliedBefore() =
        Changes.fromHistory(history).any { it.acceptNum == this.transaction.acceptNum }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("Start counting")
        leaderTimer.startCounting {
            logger.info("Recovery leader starts")
            transactionBlocker.releaseBlock()
            performProtocolAsRecoveryLeader(change)
        }
    }

    private fun leaderFailTimeoutStop() {
        logger.info("Stop counter")
        leaderTimer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: Change,
        iteration: Int
    ) {
        logger.info("Starting performing GPAC iteration: $iteration")

        val electMeResult =
            electMePhase(change, { responses -> superSet(responses, getPeersFromChange(change)) })

        if (iteration == maxLeaderElectionTries) {
            val message = "Transaction failed due to too many retries of becoming a leader."
            logger.error(message)
            signal(Signal.ReachedMaxRetries, transaction, change)
            transaction = transaction.copy(change = null)
            changeTimeout(change, message)
            return
        }

        if (!electMeResult.success) {
            retriesTimer.startCounting(iteration) {
                performProtocolAsLeader(change, iteration + 1)
            }
            return
        }

        val electResponses = electMeResult.responses

        val acceptVal =
            if (electResponses.flatten().all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        this.transaction = this.transaction.copy(acceptVal = acceptVal, acceptNum = myBallotNumber)

        applySignal(Signal.BeforeSendingAgree, this.transaction, change)

        val agreed = ftAgreePhase(change, acceptVal)
        if (!agreed) {
            return
        }

        applySignal(Signal.BeforeSendingApply, this.transaction, change)
        applyPhase(change, acceptVal)
    }

    override suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int) {
        val electMeResult = electMePhase(
            change,
            { responses -> superMajority(responses, getPeersFromChange(change)) },
            this.transaction,
            this.transaction.acceptNum
        )

        if (iteration == maxLeaderElectionTries) {
            val message = "Transaction failed due to too many retries of becoming a leader."
            logger.error(message)
            signal(Signal.ReachedMaxRetries, transaction, change)
            changeTimeout(change, message)
            transaction = transaction.copy(change = null)
            return
        }

        if (!electMeResult.success) {
            retriesTimer.startCounting(iteration) {
                performProtocolAsRecoveryLeader(change, iteration + 1)
            }
            return
        }

        val electResponses = electMeResult.responses

        val messageWithDecision = electResponses.flatten().find { it.decision }
        if (messageWithDecision != null) {
            logger.info("Got hit with message with decision true")
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = messageWithDecision.acceptVal)
            signal(Signal.BeforeSendingAgree, this.transaction, change)

            val agreed = ftAgreePhase(
                change,
                messageWithDecision.acceptVal!!,
                decision = messageWithDecision.decision,
                acceptNum = this.transaction.acceptNum
            )
            if (!agreed) {
                return
            }

            signal(Signal.BeforeSendingApply, this.transaction, change)
            applyPhase(change, messageWithDecision.acceptVal)

            return
        }

        // I got to ft-agree phase, so my voice of this is crucial
        signal(Signal.BeforeSendingAgree, this.transaction, change)

        logger.info("Recovery leader transaction state: ${this.transaction}")
        val agreed = ftAgreePhase(
            change,
            this.transaction.acceptVal!!,
            acceptNum = this.transaction.acceptNum
        )
        if (!agreed) {
            return
        }

        signal(Signal.BeforeSendingApply, this.transaction, change)
        applyPhase(change, this.transaction.acceptVal!!)

        return
    }

    data class ElectMeResult(val responses: List<List<ElectedYou>>, val success: Boolean)

    private suspend fun electMePhase(
        change: Change,
        superFunction: (List<List<ElectedYou>>) -> Boolean,
        transaction: Transaction? = null,
        acceptNum: Int? = null
    ): ElectMeResult {
        if (!history.isEntryCompatible(change.toHistoryEntry())) {
            signal(Signal.OnSendingElectBuildFail, this.transaction, change)
            changeConflicts(change)
            throw HistoryCannotBeBuildException()
        }

        myBallotNumber++
        this.transaction =
            transaction ?: Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT, change = change)

        signal(Signal.BeforeSendingElect, this.transaction, change)
        logger.info("Sending ballot number: $myBallotNumber")
        val (responses, maxBallotNumber) = getElectedYouResponses(change, getPeersFromChange(change), acceptNum)

        val electResponses: List<List<ElectedYou>> = responses
        if (superFunction(electResponses)) {
            return ElectMeResult(electResponses, true)
        }
        myBallotNumber = max(maxBallotNumber ?: 0, myBallotNumber)
        logger.info("Bumped ballot number to: $myBallotNumber")

        return ElectMeResult(electResponses, false)
    }

    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): Boolean {
        transactionBlocker.tryToBlock()

        val agreedResponses = getAgreedResponses(change, getPeersFromChange(change), acceptVal, decision, acceptNum)
        if (!superSet(agreedResponses, getPeersFromChange(change))) {
            changeTimeout(change, "Transaction failed due to too few responses of ft phase.")
            return false
        }

        this.transaction = this.transaction.copy(decision = true)
        return true
    }

    private suspend fun applyPhase(change: Change, acceptVal: Accept) {
        val applyMessages = sendApplyMessages(change, getPeersFromChange(change), acceptVal)
        logger.info("Apply Messages Responses: $applyMessages")
        this.handleApply(
            Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )
    }

    private suspend fun getElectedYouResponses(
        change: Change,
        otherPeers: List<List<PeerAddress>>,
        acceptNum: Int? = null
    ): ResponsesWithErrorAggregation<ElectedYou> =
        protocolClient.sendElectMe(
            otherPeers, ElectMe(myBallotNumber, change, acceptNum)
        )

    private suspend fun getAgreedResponses(
        change: Change,
        otherPeers: List<List<PeerAddress>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> =
        protocolClient.sendFTAgree(
            otherPeers,
            Agree(myBallotNumber, acceptVal, change, decision, acceptNum)
        )

    private suspend fun sendApplyMessages(change: Change, otherPeers: List<List<PeerAddress>>, acceptVal: Accept) =
        protocolClient.sendApply(
            otherPeers, Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )

    private fun signal(signal: Signal, transaction: Transaction?, change: Change) {
        signalPublisher.signal(
            signal,
            this,
            getPeersFromChange(change),
            transaction,
            change
        )
    }

    private fun <T> superMajority(responses: List<List<T>>, peers: List<List<T>>): Boolean =
        superFunction(responses, 2, peers)

    private fun <T> superSet(responses: List<List<T>>, peers: List<List<T>>): Boolean =
        superFunction(responses, 1, peers)

    private fun <T> superFunction(responses: List<List<T>>, divider: Int, peers: List<List<T>>): Boolean {
        val allShards = peers.size >= responses.size / divider.toDouble()
        val myPeersetId = peerResolver.currentPeerAddress().globalPeerId.peersetId

        return responses.withIndex()
            .all { (index, value) ->
                val allPeers =
                    if (index == myPeersetId) peers[index].size + 1 else peers[index].size
                val agreedPeers =
                    if (index == myPeersetId) value.size + 1 else value.size
                agreedPeers >= allPeers / 2F
            } && allShards
    }

    private fun applySignal(signal: Signal, transaction: Transaction, change: Change) {
        try {
            signal(signal, transaction, change)
        } catch (e: Exception) {
            // TODO change approach to simulating errors in signal listeners
            changeTimeout(change, e.toString())
            throw e
        }
    }

    private fun changeSucceeded(change: Change, detailedMessage: String? = null) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.SUCCESS, detailedMessage))
    }

    private fun changeConflicts(change: Change, detailedMessage: String? = null) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.CONFLICT, detailedMessage))
    }

    private fun changeTimeout(change: Change, detailedMessage: String? = null) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.TIMEOUT, detailedMessage))
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override fun getPeerName() = peerResolver.currentPeerAddress().globalPeerId.toString()
    override suspend fun performProtocol(change: Change) = performProtocolAsLeader(change)

    companion object {
        private val logger = LoggerFactory.getLogger("gpac")
    }

    override fun getLogger(): Logger = logger
    override fun getPeerResolver(): PeerResolver = peerResolver
    override fun putChangeToCompletableFutureMap(change: Change, completableFuture: CompletableFuture<ChangeResult>) {
        changeIdToCompletableFuture[change.toHistoryEntry().getId()] = completableFuture
    }

}
