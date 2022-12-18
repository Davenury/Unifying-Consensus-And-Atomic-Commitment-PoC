package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.math.max

abstract class GPACProtocolAbstract(peerResolver: PeerResolver, logger: Logger) : SignalSubject,
    AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    abstract suspend fun handleElect(message: ElectMe): ElectedYou
    abstract suspend fun handleAgree(message: Agree): Agreed
    abstract suspend fun handleApply(message: Apply)

    abstract suspend fun performProtocolAsLeader(change: Change, iteration: Int = 1)
    abstract suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int = 1)
    abstract fun getTransaction(): Transaction
    abstract fun getBallotNumber(): Int
}

class GPACProtocolImpl(
    private val history: History,
    private val gpacConfig: GpacConfig,
    ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: GPACProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    peerResolver: PeerResolver,
) : GPACProtocolAbstract(peerResolver, logger) {
    private val globalPeerId: GlobalPeerId = peerResolver.currentPeer()

    var leaderTimer: ProtocolTimer = ProtocolTimerImpl(gpacConfig.leaderFailDelay, Duration.ZERO, ctx)
    var retriesTimer: ProtocolTimer =
        ProtocolTimerImpl(gpacConfig.initialRetriesDelay, gpacConfig.retriesBackoffTimeout, ctx)
    private val maxLeaderElectionTries = gpacConfig.maxLeaderElectionTries

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)


    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

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

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

        myBallotNumber = message.ballotNumber

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

        if (message.ballotNumber != myBallotNumber) {
            throw NotValidLeader(myBallotNumber, message.ballotNumber)
        }
        logger.info("Handling agree $message")

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

        transaction =
            Transaction(
                ballotNumber = message.ballotNumber,
                change = message.change,
                acceptVal = message.acceptVal,
                initVal = initVal,
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
            if (message.acceptVal == Accept.COMMIT && !changeWasAppliedBefore(message.change)) {
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
        change.toHistoryEntry(globalPeerId.peersetId).let {
            history.addEntry(it)
        }
    }

    private fun transactionWasAppliedBefore() =
        Changes.fromHistory(history).any { it.acceptNum == this.transaction.acceptNum }

    private fun changeWasAppliedBefore(change: Change) =
        Changes.fromHistory(history).any { it.id == change.id }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("Start counting")
        leaderTimer.startCounting {
            logger.info("Recovery leader starts")
            transactionBlocker.releaseBlock()
            if (!changeWasAppliedBefore(change)) performProtocolAsRecoveryLeader(change)
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
        logger.info("$globalPeerId Starting performing GPAC iteration: $iteration")
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())

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

        try {
            applySignal(Signal.BeforeSendingApply, this.transaction, change)
        } catch (e: Exception){
            transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
            transactionBlocker.releaseBlock()
            throw e
        }
        applyPhase(change, acceptVal)
    }

    override suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int) {
        logger.info("$globalPeerId Starting performing GPAC iteration: $iteration as recovery leader")
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
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
                if (!changeWasAppliedBefore(change))
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

        logger.info("$globalPeerId Recovery leader transaction state: ${this.transaction}")
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
        if (!history.isEntryCompatible(change.toHistoryEntry(globalPeerId.peersetId))) {
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

        this.transaction = this.transaction.copy(decision = true, acceptVal = acceptVal)
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
        val myPeersetId = globalPeerId.peersetId

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
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.SUCCESS, detailedMessage))
    }

    private fun changeConflicts(change: Change, detailedMessage: String? = null) {
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.CONFLICT, detailedMessage))
    }

    private fun changeTimeout(change: Change, detailedMessage: String? = null) {
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.TIMEOUT, detailedMessage))
    }

    private fun isCurrentProcessingChangeOrApplied(change: Change): Boolean {
        val entry = change.toHistoryEntry(globalPeerId.peersetId)
        return change.id == this.transaction.change?.id || this.history.containsEntry(entry.getId())
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override suspend fun performProtocol(change: Change) = performProtocolAsLeader(change)

    companion object {
        private val logger = LoggerFactory.getLogger("gpac")
    }

}
