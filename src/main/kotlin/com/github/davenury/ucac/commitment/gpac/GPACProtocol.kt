package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.ucac.*
import com.github.davenury.ucac.commitment.AtomicCommitmentProtocol
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.history.History
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
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
    fun setPeers(peers: Map<Int, List<String>>)
    fun setMyAddress(address: String)

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

}


class GPACProtocolImpl(
    private val history: History,
    private val gpacConfig: GpacConfig,
    private val ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: GPACProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val myPeersetId: Int,
    private val myNodeId: Int,
    private var allPeers: Map<Int, List<String>>,
    private var myAddress: String
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

        val enrichedChange =
            if (change.peers.contains(myAddress)) {
                change
            } else {
                change.withAddress(myAddress)
            }
        changeIdToCompletableFuture[enrichedChange.toHistoryEntry().getId()] = cf

        GlobalScope.launch {
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

        transactionBlocker.assertICanSendElectedYou()

        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(myBallotNumber, message.ballotNumber)
        val initVal = if (history.isEntryCompatible(message.change.toHistoryEntry())) Accept.COMMIT else Accept.ABORT

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
        logger.info(message.toString())
        this.transaction =
            this.transaction.copy(
                ballotNumber = message.ballotNumber,
                acceptVal = message.acceptVal,
                acceptNum = message.acceptNum ?: message.ballotNumber
            )

        logger.info("${getPeerName()} state transaction state: ${this.transaction}")

        myBallotNumber = message.ballotNumber

        if (!message.decision) {
            transactionBlocker.tryToBlock()
            logger.info("${getPeerName()} Lock aquired: ${message.ballotNumber}")
        }

        signal(Signal.OnHandlingAgreeEnd, transaction, message.change)

        leaderFailTimeoutStart(message.change)

        return Agreed(transaction.ballotNumber, message.acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        logger.info("${getPeerName()} - HandleApply message: $message")
        val isCurrentTransaction = message.ballotNumber == this.myBallotNumber

        if (isCurrentTransaction) leaderFailTimeoutStop()
        signal(Signal.OnHandlingApplyBegin, transaction, message.change)

        val changeId = message.change.toHistoryEntry().getId()
        try {
            if (isCurrentTransaction) {
                this.transaction =
                    this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)
            }

            if (message.acceptVal == Accept.COMMIT) {
                signal(Signal.OnHandlingApplyCommitted, transaction, message.change)
            }
            if (message.acceptVal == Accept.COMMIT && !transactionWasAppliedBefore()) {
                history.addEntry(message.change.toHistoryEntry())
            }
            changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.SUCCESS))
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT, change = message.change)

            logger.info("${getPeerName()} Releasing semaphore as cohort")
            transactionBlocker.releaseBlock()

            changeConflicts(message.change)
            signal(Signal.OnHandlingApplyEnd, transaction, message.change)
        }


    }

    private fun transactionWasAppliedBefore() =
        Changes.fromHistory(history).any { it.acceptNum == this.transaction.acceptNum }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("${getPeerName()} Start counting")
        leaderTimer.startCounting {
            logger.info("${getPeerName()} Recovery leader starts")
            transactionBlocker.releaseBlock()
            performProtocolAsRecoveryLeader(change)
        }
    }

    private fun leaderFailTimeoutStop() {
        logger.info("${getPeerName()} Stop counter")
        leaderTimer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: Change,
        iteration: Int
    ) {
        logger.info("Peer ${getPeerName()} starts performing GPAC iteration: $iteration")
        println("Start protocol")

        val electMeResult =
            electMePhase(change, { responses -> superSet(responses, getPeersFromChange(change)) })

        if (iteration == maxLeaderElectionTries) {
            logger.error("Transaction failed due to too many retries of becoming a leader.")
            signal(Signal.ReachedMaxRetries, transaction, change)
            transaction = transaction.copy(change = null)
            changeConflicts(change, MaxTriesExceededException())
            throw MaxTriesExceededException()
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

        val agreedResponses = ftAgreePhase(change, acceptVal)

        applySignal(Signal.BeforeSendingApply, this.transaction, change)

        val applyResponses = applyPhase(change, acceptVal)
    }

    override suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int) {
        val electMeResult = electMePhase(
            change,
            { responses -> superMajority(responses, getPeersFromChange(change)) },
            this.transaction,
            this.transaction.acceptNum
        )

        if (iteration == maxLeaderElectionTries) {
            logger.error("Transaction failed due to too many retries of becoming a leader.")
            signal(Signal.ReachedMaxRetries, transaction, change)
            changeConflicts(change)
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
            logger.info("${getPeerName()} Got hit with message with decision true")
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = messageWithDecision.acceptVal)
            signal(Signal.BeforeSendingAgree, this.transaction, change)

            val agreedResponses = ftAgreePhase(
                change,
                messageWithDecision.acceptVal!!,
                decision = messageWithDecision.decision,
                acceptNum = this.transaction.acceptNum
            )

            signal(Signal.BeforeSendingApply, this.transaction, change)

            val applyResponses = applyPhase(change, messageWithDecision.acceptVal)

            return
        }

        // I got to ft-agree phase, so my voice of this is crucial
        signal(Signal.BeforeSendingAgree, this.transaction, change)

        logger.info("${getPeerName()} Recovery leader transaction state: ${this.transaction}")
        val agreedResponses = ftAgreePhase(
            change,
            this.transaction.acceptVal!!,
            acceptNum = this.transaction.acceptNum
        )

        signal(Signal.BeforeSendingApply, this.transaction, change)

        val applyResponses = applyPhase(change, this.transaction.acceptVal!!)

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
        logger.info("${getPeerName()} - sending ballot number: $myBallotNumber")
        val (responses, maxBallotNumber) = getElectedYouResponses(change, getPeersFromChange(change), acceptNum)

        val electResponses: List<List<ElectedYou>> = responses
        if (superFunction(electResponses)) {
            return ElectMeResult(electResponses, true)
        }
        myBallotNumber = max(maxBallotNumber ?: 0, myBallotNumber)
        logger.info("${getPeerName()} Bumped ballot number to: $myBallotNumber")

        return ElectMeResult(electResponses, false)
    }

    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> {
        transactionBlocker.tryToBlock()

        val agreedResponses = getAgreedResponses(change, getPeersFromChange(change), acceptVal, decision, acceptNum)
        if (!superSet(agreedResponses, getPeersFromChange(change))) {
            TooFewResponsesException().also { changeConflicts(change, it) }.let { throw it }
        }

        this.transaction = this.transaction.copy(decision = true)
        return agreedResponses
    }

    private suspend fun applyPhase(change: Change, acceptVal: Accept): List<Int> {
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
        return applyMessages
    }

    private suspend fun getElectedYouResponses(
        change: Change,
        otherPeers: List<List<String>>,
        acceptNum: Int? = null
    ): ResponsesWithErrorAggregation<ElectedYou> =
        protocolClient.sendElectMe(
            otherPeers, ElectMe(myBallotNumber, change, acceptNum)
        )

    private suspend fun getAgreedResponses(
        change: Change,
        otherPeers: List<List<String>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> =
        protocolClient.sendFTAgree(
            otherPeers,
            Agree(myBallotNumber, acceptVal, change, decision, acceptNum)
        )

    private suspend fun sendApplyMessages(change: Change, otherPeers: List<List<String>>, acceptVal: Accept) =
        protocolClient.sendApply(
            otherPeers, Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )

    private fun signal(signal: Signal, transaction: Transaction?, change: Change) {
        signalPublisher.signal(signal, this, getPeersFromChange(change), transaction, change)
    }

    private fun <T> superMajority(responses: List<List<T>>, peers: List<List<String>>): Boolean =
        superFunction(responses, 2, peers)

    private fun <T> superSet(responses: List<List<T>>, peers: List<List<String>>): Boolean =
        superFunction(responses, 1, peers)

    private fun <T> superFunction(responses: List<List<T>>, divider: Int, peers: List<List<String>>): Boolean {
        val allShards = peers.size >= responses.size / divider.toDouble()

        return responses.withIndex()
            .all { (index, value) ->
                val allPeers =
                    if (index + 1 == myPeersetId) peers[index].size + 1 else peers[index].size
                val agreedPeers =
                    if (index + 1 == myPeersetId) value.size + 1 else value.size
                agreedPeers >= allPeers / 2F
            } && allShards
    }

    private fun applySignal(signal: Signal, transaction: Transaction, change: Change) {
        try {
            signal(signal, transaction, change)
        } catch (e: Exception) {
            changeConflicts(change, e)
            throw e
        }
    }

    private fun changeConflicts(change: Change) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.CONFLICT))
    }

    private fun changeConflicts(change: Change, exception: Exception) {
        val changeId = change.toHistoryEntry().getId()
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(ChangeResult.Status.EXCEPTION, exception))
    }

    private fun getPeersFromChange(change: Change): List<List<String>> {
        if (change.peers.isEmpty()) throw RuntimeException("Change without peers")
        return change.peers.map { peer ->
            if (peer == myAddress) return@map allPeers[myPeersetId]!!
            allPeers.values.find { it.contains(peer) }
                ?: throw PeerNotInPeersetException(peer).also { changeConflicts(change, it) }
        }
    }

    override fun setPeers(peers: Map<Int, List<String>>) {
        this.allPeers = peers
    }

    override fun setMyAddress(address: String) {
        this.myAddress = address
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]


    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }

    override fun getPeerName() = "peerset${myPeersetId}/peer${myNodeId}"
}
