package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.math.floor
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
    private val isMetricTest: Boolean,
) : GPACProtocolAbstract(peerResolver, logger) {
    private val globalPeerId: GlobalPeerId = peerResolver.currentPeer()

    var leaderTimer: ProtocolTimer = ProtocolTimerImpl(gpacConfig.leaderFailDelay, gpacConfig.leaderFailBackoff, ctx)
    var retriesTimer: ProtocolTimer =
        ProtocolTimerImpl(gpacConfig.initialRetriesDelay, gpacConfig.retriesBackoffTimeout, ctx)
    private val maxLeaderElectionTries = gpacConfig.maxLeaderElectionTries

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
    private val phaseMutex = Mutex()
    private var currentPhase: GPACPhase? = null


    private fun isValidBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

    private fun GPACPhase?.shouldExecutePhaseIGotNow(newPhase: GPACPhase): Boolean {
        if (this == null) {
            return true
        }
        return this.shouldExecutePhaseIGotNow(newPhase)
    }

    override suspend fun handleElect(message: ElectMe): ElectedYou {
        phaseMutex.withLock {
            if (!currentPhase.shouldExecutePhaseIGotNow(GPACPhase.ELECT) || message.ballotNumber < myBallotNumber) {
                throw IllegalStateException("I should not execute this phase, since the phase I've executed is: $currentPhase; they want me to do ${GPACPhase.ELECT}")
            }
            currentPhase = GPACPhase.ELECT
        }
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

        if (transactionBlocker.isAcquired() && transactionBlocker.getChangeId() != message.change.id) {
            logger.info("Tried to respond to elect me when semaphore acquired!")
            throw AlreadyLockedException(ProtocolName.GPAC)
        }

        if (!isValidBallotNumber(message.ballotNumber)) {
            throw NotElectingYou(myBallotNumber, message.ballotNumber)
        }

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        var initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT
        if (gpacConfig.abortOnElectMe) {
            initVal = Accept.ABORT
        }

        myBallotNumber = message.ballotNumber

        signal(Signal.OnHandlingElectEnd, transaction, message.change)
        this.transaction = transaction.copy(initVal = initVal)

        if (isMetricTest) {
            Metrics.bumpChangeMetric(
                changeId = message.change.id,
                peerId = peerResolver.currentPeer().peerId,
                peersetId = peerResolver.currentPeer().peersetId,
                protocolName = ProtocolName.GPAC,
                state = "leader_elected"
            )
        }
        return ElectedYou(
            message.ballotNumber,
            initVal,
            transaction.acceptNum,
            transaction.acceptVal,
            transaction.decision
        )
    }

    override suspend fun handleAgree(message: Agree): Agreed {
        phaseMutex.withLock {
            if (!currentPhase.shouldExecutePhaseIGotNow(GPACPhase.AGREE) || message.ballotNumber < myBallotNumber) {
                return Agreed(
                    message.ballotNumber,
                    acceptVal = this.transaction.acceptVal!!
                )
            }
            currentPhase = GPACPhase.AGREE
        }

        signal(Signal.OnHandlingAgreeBegin, transaction, message.change)

        if (message.ballotNumber < myBallotNumber) {
            throw NotValidLeader(myBallotNumber, message.ballotNumber)
        }
        logger.info("Handling agree $message")

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

        myBallotNumber = message.ballotNumber

        if (!this.transaction.decision) {
            try {
                transactionBlocker.tryToBlock(ProtocolName.GPAC, message.change.id)
            } catch (e: Exception) {
                return Agreed(
                    ballotNumber = message.ballotNumber,
                    acceptVal = Accept.ABORT,
                )
            }
        }
        logger.info("Lock aquired: ${message.ballotNumber}")

        transaction =
            Transaction(
                ballotNumber = message.ballotNumber,
                change = message.change,
                acceptVal = message.acceptVal,
                initVal = initVal,
                acceptNum = message.acceptNum ?: message.ballotNumber
            )

        logger.info("State transaction state: ${this.transaction}")


        signal(Signal.OnHandlingAgreeEnd, transaction, message.change)

        if (isMetricTest) {
            Metrics.bumpChangeMetric(
                changeId = message.change.id,
                peerId = peerResolver.currentPeer().peerId,
                peersetId = peerResolver.currentPeer().peersetId,
                protocolName = ProtocolName.GPAC,
                state = "agreed"
            )
        }
        leaderFailTimeoutStart(message.change)

        return Agreed(transaction.ballotNumber, message.acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        phaseMutex.withLock {
            if (!currentPhase.shouldExecutePhaseIGotNow(GPACPhase.APPLY) || message.ballotNumber < myBallotNumber) {
                throw IllegalStateException("I should not execute this phase, since the phase I've executed is: $currentPhase; they want me to do ${GPACPhase.APPLY}")
            }
            currentPhase = GPACPhase.APPLY
        }
        logger.info("HandleApply message: $message")
        val isCurrentTransaction =
            message.ballotNumber >= this.myBallotNumber

        if (isCurrentTransaction) leaderFailTimeoutStop()
        signal(Signal.OnHandlingApplyBegin, transaction, message.change)

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)

        when {
            !isCurrentTransaction && !transactionBlocker.isAcquired() -> {

                if (!history.containsEntry(entry.getId()))
                    transactionBlocker.tryToBlock(ProtocolName.GPAC, message.change.id)

                transaction =
                    Transaction(
                        ballotNumber = message.ballotNumber,
                        change = message.change,
                        acceptVal = message.acceptVal,
                        initVal = message.acceptVal,
                        acceptNum = message.ballotNumber
                    )
            }

            !isCurrentTransaction -> {
                logger.info(" is not blocked")
                changeConflicts(message.change, "Don't receive ft-agree and can't block on history")
                throw TransactionNotBlockedOnThisChange(ProtocolName.GPAC, message.change.id)
            }
        }

        try {
            this.transaction =
                this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)


            val (changeResult, resultMessage) = if (message.acceptVal == Accept.COMMIT && !history.containsEntry(entry.getId())) {
                addChangeToHistory(message.change)
                signal(Signal.OnHandlingApplyCommitted, transaction, message.change)
                Pair(ChangeResult.Status.SUCCESS, null)
            } else if (message.acceptVal == Accept.ABORT) {
                Pair(ChangeResult.Status.ABORTED, "Message was applied but state was ABORT")
            } else {
                Pair(ChangeResult.Status.SUCCESS, null)
            }

            logger.info("handleApply releaseBlocker")

            changeResult.resolveChange(message.change.id, resultMessage)
            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = message.change.id,
                    peerId = peerResolver.currentPeer().peerId,
                    peersetId = peerResolver.currentPeer().peersetId,
                    protocolName = ProtocolName.GPAC,
                    state = changeResult.name.lowercase()
                )
            }
        } catch (ex: Exception) {
            logger.error("Exception during applying change, set it to abort", ex)
            transaction =
                transaction.copy(ballotNumber = myBallotNumber, decision = true, initVal = Accept.ABORT, change = null)
        } finally {
            logger.info("handleApply finally releaseBlocker")
            if (transactionBlocker.isAcquired()) transactionBlocker.tryToReleaseBlockerChange(
                ProtocolName.GPAC,
                message.change.id
            )

            signal(Signal.OnHandlingApplyEnd, transaction, message.change)
        }
    }

    private fun addChangeToHistory(change: Change) {
        change.toHistoryEntry(globalPeerId.peersetId).let {
            history.addEntry(it)
        }
    }

    private fun changeWasAppliedBefore(change: Change) =
        Changes.fromHistory(history).any { it.id == change.id }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("Start counting")
        leaderTimer.startCounting {
            logger.info("Recovery leader starts")
            logger.info("leaderFailTimeout releaseBlocker")
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
        logger.info("Starting performing GPAC iteration: $iteration")
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())

        try {
            val electMeResult =
                electMePhase(change, { responses ->
                    superSet(responses, getPeersFromChange(change)) { it.initVal == Accept.COMMIT } ||
                            superSet(responses, getPeersFromChange(change)) { it.initVal == Accept.ABORT }
                })

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

            val acceptVal = electResponses.getAcceptVal(change)

            if (acceptVal == null) {
                retriesTimer.startCounting(iteration) {
                    performProtocolAsLeader(change, iteration + 1)
                }
                return
            }

            this.transaction = this.transaction.copy(acceptVal = acceptVal, acceptNum = myBallotNumber)

            applySignal(Signal.BeforeSendingAgree, this.transaction, change)

            val agreed = ftAgreePhase(change, acceptVal)
            if (!agreed) {
                return
            }

            try {
                applySignal(Signal.BeforeSendingApply, this.transaction, change)
            } catch (e: Exception) {
                transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
                logger.info("performProtocolAsLeader releaseBlocker")
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id)
                throw e
            }
            applyPhase(change, acceptVal)
        } catch (e: Exception) {
            logger.error("Error while performing protocol as leader for change ${change.id}", e)
            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT, e.message))
        }
    }

    private fun List<List<ElectedYou>>.getAcceptVal(change: Change): Accept? {

        val shouldCommit = superSet(this, getPeersFromChange(change)) { (it as ElectedYou).initVal == Accept.COMMIT }
        val shouldAbort = superSet(this, getPeersFromChange(change)) { (it as ElectedYou).initVal == Accept.ABORT }

        return when {
            shouldCommit -> Accept.COMMIT
            shouldAbort -> Accept.ABORT
            else -> null
        }
    }

    override suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int) {
        logger.info("Starting performing GPAC iteration: $iteration as recovery leader")
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
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id)
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
        if (!history.isEntryCompatible(change.toHistoryEntry(globalPeerId.peersetId))) {
            signal(Signal.OnSendingElectBuildFail, this.transaction, change)
            changeRejected(
                change,
                "History entry not compatible, change: ${change}, expected: ${history.getCurrentEntryId()}"
            )
            throw HistoryCannotBeBuildException()
        }

        myBallotNumber++
        this.transaction =
            transaction ?: Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT, change = change)

        signal(Signal.BeforeSendingElect, this.transaction, change)
        logger.info("Sending ballot number: $myBallotNumber")
        val responses = getElectedYouResponses(change, getPeersFromChange(change), acceptNum)

        val (electResponses: List<List<ElectedYou>>, success: Boolean) =
            GPACResponsesContainer(responses, Duration.ofSeconds(2)).awaitForMessages { superFunction(it) }

        if (success) {
            return ElectMeResult(electResponses, true)
        }

        myBallotNumber++
        logger.info("Bumped ballot number to: $myBallotNumber")

        return ElectMeResult(electResponses, false)
    }

    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null,
        iteration: Int = 0,
    ): Boolean {

        transactionBlocker.tryToBlock(ProtocolName.GPAC, change.id)

        val responses = getAgreedResponses(change, getPeersFromChange(change), acceptVal, decision, acceptNum)

        val (_: List<List<Agreed>>, success: Boolean) =
            GPACResponsesContainer(responses, Duration.ofSeconds(2)).awaitForMessages { superSet(it, getPeersFromChange(change)) }

        if (!success && iteration == gpacConfig.maxFTAgreeTries) {
            changeTimeout(change, "Transaction failed due to too few responses of ft phase.")
            transactionBlocker.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id)
            return false
        }

        if (!success) {
            delay(gpacConfig.ftAgreeRepeatDelay.toMillis())
            return ftAgreePhase(change, acceptVal, decision, acceptNum, iteration + 1)
        }

        this.transaction = this.transaction.copy(decision = true, acceptVal = acceptVal)
        return true
    }

    private suspend fun applyPhase(change: Change, acceptVal: Accept) {
        val applyMessages = sendApplyMessages(change, getPeersFromChange(change), acceptVal)

        val (responses, success) = GPACResponsesContainer(applyMessages, Duration.ofSeconds(2)).awaitForMessages {
            if (gpacConfig.waitForAllInApply) {
                it.flatten().size == getPeersFromChange(change).flatten().size
            } else {
                superSet(it, getPeersFromChange(change))
            }
        }

        logger.info("Responses from apply: ${responses.flatten().map { 
            try {
                it.status   
            } catch (e: Exception) {
                "null"
            }
        }}")
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
    ): List<List<Deferred<ElectedYou?>>> =
        protocolClient.sendElectMe(
            otherPeers, ElectMe(myBallotNumber, change, acceptNum)
        )

    private suspend fun getAgreedResponses(
        change: Change,
        otherPeers: List<List<PeerAddress>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Deferred<Agreed?>>> =
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

    private fun <T> superMajority(responses: List<List<T>>, peers: List<List<PeerAddress>>): Boolean =
        responses.size.isMoreThanHalfOf(peers.size) && superFunction(responses, peers)

    private fun <T> superSet(responses: List<List<T>>, peers: List<List<PeerAddress>>, condition: (T) -> Boolean = { true }): Boolean =
        (peers.size == responses.size) && superFunction(responses, peers, condition)

    private fun <T> superFunction(responses: List<List<T>>, peers: List<List<PeerAddress>>, condition: (T) -> Boolean = { true }): Boolean {
        val myPeersetId = globalPeerId.peersetId

        return responses.withIndex()
            .all { (index, responses) ->
                val allPeers =
                    if (index == myPeersetId) peers[index].size + 1 else peers[index].size
                val agreedPeers =
                    if (index == myPeersetId) {
                        responses.count { condition(it) } + 1
                    } else {
                        responses.count { condition(it) }
                    }
                agreedPeers >= (floor(allPeers * 0.5) + 1)
                agreedPeers.isMoreThanHalfOf(allPeers)
            }
    }

    private fun Int.isMoreThanHalfOf(otherValue: Int) =
        this >= (floor(otherValue * 0.5) + 1)

    private fun applySignal(signal: Signal, transaction: Transaction, change: Change) {
        try {
            signal(signal, transaction, change)
        } catch (e: Exception) {
            // TODO change approach to simulating errors in signal listeners
            changeTimeout(change, e.toString())
            throw e
        }
    }

    private fun changeRejected(change: Change, detailedMessage: String? = null) {
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.REJECTED, detailedMessage))
    }

    private fun changeConflicts(change: Change, detailedMessage: String? = null) {
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.CONFLICT, detailedMessage))
    }

    private fun changeTimeout(change: Change, detailedMessage: String? = null) {
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.TIMEOUT, detailedMessage))
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]

    override suspend fun performProtocol(change: Change) = performProtocolAsLeader(change)

    companion object {
        private val logger = LoggerFactory.getLogger("gpac")
    }

    private fun ChangeResult.Status.resolveChange(changeId: String, detailedMessage: String? = null) {
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(this, detailedMessage))
    }

}
