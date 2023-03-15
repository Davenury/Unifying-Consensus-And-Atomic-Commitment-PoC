package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.common.TransactionBlocker
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

abstract class GPACProtocolAbstract(peerResolver: PeerResolver, logger: Logger) : SignalSubject,
    AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    abstract suspend fun handleElect(message: ElectMe): ElectedYou
    abstract suspend fun handleAgree(message: Agree): Agreed
    abstract suspend fun handleApply(message: Apply): Applied

    abstract suspend fun handleElectResponse(message: ElectedYou)
    abstract suspend fun handleAgreeResponse(message: Agreed)
    abstract suspend fun handleApplyResponse(message: Applied)

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
    private val gpacResponsesContainer: GPACResponsesContainer,
) : GPACProtocolAbstract(peerResolver, logger) {
    private val globalPeerId: GlobalPeerId = peerResolver.currentPeer()

    var leaderTimer: ProtocolTimer = ProtocolTimerImpl(gpacConfig.leaderFailDelay,gpacConfig.leaderFailBackoff, ctx)

    var retriesTimer: ProtocolTimer =
        ProtocolTimerImpl(gpacConfig.initialRetriesDelay, gpacConfig.retriesBackoffTimeout, ctx)
    private val maxLeaderElectionTries = gpacConfig.maxLeaderElectionTries

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)


    private fun isValidBallotNumber(ballotNumber: Int): Boolean =
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
                change = message.change,
                message.ballotNumber,
                Accept.COMMIT,
                message.acceptNum,
                Accept.COMMIT,
                true,
                elected = true,
                sender = peerResolver.currentPeer()
            )
        }

        signal(Signal.OnHandlingElectBegin, null, message.change)

        if (transactionBlocker.isAcquired() && transactionBlocker.getChangeId() != message.change.id) {
            logger.info("Tried to respond to elect me when semaphore acquired!")
            return ElectedYou(
                change = message.change,
                message.ballotNumber,
                Accept.ABORT,
                message.acceptNum ?: 0,
                Accept.ABORT,
                false,
                elected = false,
                reason = Reason.ALREADY_LOCKED,
                sender = peerResolver.currentPeer()
            )
        }

        if (!isValidBallotNumber(message.ballotNumber)) {
            return ElectedYou(
                change = message.change,
                myBallotNumber,
                Accept.ABORT,
                message.acceptNum ?: 0,
                Accept.ABORT,
                decision = false,
                elected = false,
                Reason.WRONG_BALLOT_NUMBER,
                sender = peerResolver.currentPeer()
            )
        }

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

        myBallotNumber = message.ballotNumber

        signal(Signal.OnHandlingElectEnd, transaction, message.change)

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
            change = message.change,
            message.ballotNumber,
            initVal,
            transaction.acceptNum,
            transaction.acceptVal,
            transaction.decision,
            elected = true,
            sender = peerResolver.currentPeer()
        )
    }

    override suspend fun handleAgree(message: Agree): Agreed {

        signal(Signal.OnHandlingAgreeBegin, transaction, message.change)

        if (message.ballotNumber < myBallotNumber) {
            logger.error("Not valid leader - my ballot number: $myBallotNumber, provided: ${message.ballotNumber}")
            return Agreed(
                change = message.change,
                ballotNumber = message.ballotNumber,
                acceptVal = Accept.ABORT,
                agreed = false,
                reason = Reason.NOT_VALID_LEADER,
                sender = peerResolver.currentPeer()
            )
        }
        logger.info("Handling agree $message")

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)
        val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

        myBallotNumber = message.ballotNumber

        if (!history.containsEntry(entry.getId())) {
            try {
                transactionBlocker.tryToBlock(ProtocolName.GPAC, message.change.id)
            } catch (e: Exception) {
                return Agreed(
                    change = message.change,
                    ballotNumber = message.ballotNumber,
                    acceptVal = Accept.ABORT,
                    agreed = false,
                    reason = Reason.ALREADY_LOCKED,
                    sender = peerResolver.currentPeer()
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

        return Agreed(
            change = message.change,
            transaction.ballotNumber,
            message.acceptVal,
            agreed = true,
            sender = peerResolver.currentPeer()
        )
    }

    override suspend fun handleApply(message: Apply): Applied {
        logger.info("HandleApply message: $message")
        val isCurrentTransaction =
            message.ballotNumber >= this.myBallotNumber

        if (isCurrentTransaction) leaderFailTimeoutStop()
        signal(Signal.OnHandlingApplyBegin, transaction, message.change)

        val entry = message.change.toHistoryEntry(globalPeerId.peersetId)

        when {
            !isCurrentTransaction && !transactionBlocker.isAcquired() -> {

                if (history.containsEntry(entry.getId()))
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
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)

            logger.info("handleApply finally releaseBlocker")
            if(transactionBlocker.isAcquired()) transactionBlocker.tryToReleaseBlockerChange(ProtocolName.GPAC, message.change.id)

            signal(Signal.OnHandlingApplyEnd, transaction, message.change)

        }

        return Applied(
            change = message.change,
            sender = peerResolver.currentPeer()
        )
    }

    private fun addChangeToHistory(change: Change) {
        change.toHistoryEntry(globalPeerId.peersetId).let {
            history.addEntry(it)
        }
    }

    private fun changeWasAppliedBefore(change: Change) =
        Changes.fromHistory(history).any { it.id == change.id }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("Start counting ${peerResolver.currentPeer()}")
        leaderTimer.startCounting {
            logger.info("Recovery leader starts ${peerResolver.currentPeer()}")
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
            } catch (e: Exception) {
                transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
                logger.info("performProtocolAsLeader releaseBlocker")
                transactionBlocker.tryToReleaseBlockerChange(ProtocolName.GPAC, change.id)
                throw e
            }
            applyPhase(change, acceptVal)
        } catch (e: Exception) {
            logger.error("Error while performing gpac", e)
            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT, e.message))
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

        sendElectMessages(change, getPeersFromChange(change), acceptNum)

        val responses = gpacResponsesContainer.waitForElectResponses { responses ->
            superFunction(responses) || areListsEqualInSize(responses, getPeersFromChange(change))
        }

        if (superFunction(responses)) {
            return ElectMeResult(responses, true)
        }

        return responses
            .flatten()
            .filter { it.reason != null }
            .let {
                return@let when {
                    it.any { it.reason == Reason.ALREADY_LOCKED } -> Reason.ALREADY_LOCKED
                    it.any { it.reason == Reason.WRONG_BALLOT_NUMBER } -> Reason.WRONG_BALLOT_NUMBER
                    else -> Reason.UNKNOWN
                }
            }
            .resolveElectedYou(responses)
    }

    private fun Reason.resolveElectedYou(responses: List<List<ElectedYou>>): ElectMeResult =
        when (this) {
            Reason.WRONG_BALLOT_NUMBER -> kotlin.run {
                myBallotNumber = responses.flatten().maxOf { it.ballotNumber }
                logger.info("Bumped ballot number to: $myBallotNumber")
                return ElectMeResult(responses, false)
            }

            else -> kotlin.run {
                logger.info("GPAC elect phase ended with $this, executing once more")
                ElectMeResult(responses, false)
            }
        }


    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null,
        iteration: Int = 0,
    ): Boolean {

        logger.info("Starting ft agree with iteration: $iteration")
        if (iteration == gpacConfig.maxFTAgreeTries) {
            return false
        }

        transactionBlocker.tryToBlock(ProtocolName.GPAC, change.id)

        sendFtAgreeMessages(change, getPeersFromChange(change), acceptVal, decision, acceptNum)

        val responses = gpacResponsesContainer.waitForAgreeResponses { responses ->
            superSet(responses, getPeersFromChange(change)) || areListsEqualInSize(
                responses,
                getPeersFromChange(change)
            )
        }

        if (superSet(responses, getPeersFromChange(change))) {
            this.transaction = this.transaction.copy(decision = true, acceptVal = acceptVal)
            return true
        }

        val reason = responses
            .flatten()
            .filter { it.reason != null }
            .let {
                return@let when {
                    it.any { it.reason == Reason.NOT_VALID_LEADER } -> Reason.NOT_VALID_LEADER
                    it.any { it.reason == Reason.ALREADY_LOCKED } -> Reason.ALREADY_LOCKED
                    else -> Reason.UNKNOWN
                }
            }

        if (reason == Reason.NOT_VALID_LEADER) {
            logger.error("Discarding GPAC for change: ${change.id} as it is not valid leader anymore")
            return false
        }

        delay(gpacConfig.ftAgreeRepeatDelay.toMillis())
        return ftAgreePhase(change, acceptVal, decision, acceptNum, iteration + 1)
    }

    private suspend fun applyPhase(change: Change, acceptVal: Accept) {
        sendApplyMessages(change, getPeersFromChange(change), acceptVal)
        gpacResponsesContainer.waitForApplyResponses {
            superSet(it, getPeersFromChange(change)) || areListsEqualInSize(it, getPeersFromChange(change))
        }
        this.handleApply(
            Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )
    }

    private suspend fun sendElectMessages(
        change: Change,
        otherPeers: List<List<PeerAddress>>,
        acceptNum: Int? = null
    ) {
        protocolClient.sendElectMe(
            otherPeers, ElectMe(myBallotNumber, change, acceptNum)
        )
    }

    private suspend fun sendFtAgreeMessages(
        change: Change,
        otherPeers: List<List<PeerAddress>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ) {
        protocolClient.sendFTAgree(
            otherPeers,
            Agree(myBallotNumber, acceptVal, change, decision, acceptNum)
        )
    }

    private suspend fun sendApplyMessages(change: Change, otherPeers: List<List<PeerAddress>>, acceptVal: Accept) {
        protocolClient.sendApply(
            otherPeers, Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )
    }

    private fun signal(signal: Signal, transaction: Transaction?, change: Change) {
        signalPublisher.signal(
            signal,
            this,
            getPeersFromChange(change),
            transaction,
            change
        )
    }

    private fun <T : GpacResponse> superMajority(responses: List<List<T>>, peers: List<List<PeerAddress>>): Boolean =
        superFunction(responses, 2, peers)

    private fun <T : GpacResponse> superSet(responses: List<List<T>>, peers: List<List<PeerAddress>>): Boolean =
        superFunction(responses, 1, peers)

    private fun <T : GpacResponse> superFunction(
        responses: List<List<T>>,
        divider: Int,
        peers: List<List<PeerAddress>>
    ): Boolean {
        // TODO - why concurrent modification exception - while was already out of condition, got response from another peer
        val allShards = responses.size >= peers.size / divider.toDouble()
        val myPeersetId = globalPeerId.peersetId

        return peers.withIndex()
            .all { (index, peers) ->
                val allPeers =
                    if (index == myPeersetId) {
                        peers.size + 1
                    } else {
                        peers.size
                    }
                val agreedPeers =
                    if (index == myPeersetId) {
                        responses.getOrElse(index, { listOf() }).count { it.isSuccess() } + 1
                    } else {
                        responses.getOrElse(index, { listOf() }).count { it.isSuccess() }
                    }
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

    override suspend fun handleElectResponse(message: ElectedYou) {
        gpacResponsesContainer.addElectResponse(message)
    }

    override suspend fun handleAgreeResponse(message: Agreed) {
        gpacResponsesContainer.addAgreeResponse(message)
    }

    override suspend fun handleApplyResponse(message: Applied) {
        gpacResponsesContainer.addApplyResponse(message)
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
