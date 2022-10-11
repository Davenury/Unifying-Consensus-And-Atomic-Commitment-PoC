package com.github.davenury.ucac.gpac.domain

import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.*
import org.slf4j.LoggerFactory
import kotlin.math.max

interface GPACProtocol : SignalSubject {
    suspend fun handleElect(message: ElectMe): ElectedYou
    suspend fun handleAgree(message: Agree): Agreed
    suspend fun handleApply(message: Apply)
    suspend fun performProtocolAsLeader(change: Change)
    suspend fun performProtocolAsRecoveryLeader(change: Change)
    fun getTransaction(): Transaction
    fun getBallotNumber(): Int
    fun setPeers(peers: Map<Int, List<String>>)
    fun setMyAddress(address: String)
}

class GPACProtocolImpl(
    private val historyManagement: HistoryManagement,
    private val maxLeaderElectionTries: Int,
    private val timer: ProtocolTimer,
    private val protocolClient: GPACProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap()),
    private val myPeersetId: Int,
    private val myNodeId: Int,
    private var allPeers: Map<Int, List<String>>,
    private var myAddress: String
) : GPACProtocol {

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT)

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

    override suspend fun handleElect(message: ElectMe): ElectedYou {
        val state = historyManagement.getState()
        val decision = message.acceptNum?.let { acceptNum ->
            state.find { it.acceptNum == acceptNum }
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

        signal(Signal.OnHandlingElectBegin, null, getPeersFromChange(message.change))

        transactionBlocker.assertICanSendElectedYou()

        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(myBallotNumber, message.ballotNumber)
        val initVal = if (historyManagement.canBeBuild(message.change)) Accept.COMMIT else Accept.ABORT

        transaction = Transaction(ballotNumber = message.ballotNumber, initVal = initVal)

        signal(Signal.OnHandlingElectEnd, transaction, getPeersFromChange(message.change))

        return ElectedYou(
            message.ballotNumber,
            initVal,
            transaction.acceptNum,
            transaction.acceptVal,
            transaction.decision
        )
    }

    override suspend fun handleAgree(message: Agree): Agreed {

        signal(Signal.OnHandlingAgreeBegin, transaction, getPeersFromChange(message.change))

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

        signal(Signal.OnHandlingAgreeEnd, transaction, getPeersFromChange(message.change))

        leaderFailTimeoutStart(message.change)

        return Agreed(transaction.ballotNumber, message.acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        leaderFailTimeoutStop()
        signal(Signal.OnHandlingApplyBegin, transaction, getPeersFromChange(message.change))

        try {
            this.transaction =
                this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)

            logger.info("${getPeerName()} - my state: ${historyManagement.getState()}")

            if (message.acceptVal == Accept.COMMIT) {
                signal(Signal.OnHandlingApplyCommitted, transaction, getPeersFromChange(message.change))
            }
            if (message.acceptVal == Accept.COMMIT && !transactionWasAppliedBefore()) {
                historyManagement.change(message.change)
            }
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT)

            logger.info("${getPeerName()} Releasing semaphore as cohort")
            transactionBlocker.releaseBlock()

            signal(Signal.OnHandlingApplyEnd, transaction, getPeersFromChange(message.change))
        }
    }

    private fun transactionWasAppliedBefore() =
        historyManagement.getState().any { it.acceptNum == this.transaction.acceptNum }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.info("${getPeerName()} Start counting")
        timer.startCounting {
            logger.info("${getPeerName()} Recovery leader starts")
            transactionBlocker.releaseBlock()
            performProtocolAsRecoveryLeader(change)
        }
    }

    private fun leaderFailTimeoutStop() {
        logger.info("${getPeerName()} Stop counter")
        timer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: Change
    ) {
        logger.info("Peer ${getPeerName()} starts performing GPAC")
        val enrichedChange =
            if (change.peers.contains(myAddress)) {
                change
            } else {
                change.withAddress(myAddress)
            }
        val electResponses = electMePhase(enrichedChange, { responses -> superSet(responses, getPeersFromChange(enrichedChange)) })

        val acceptVal =
            if (electResponses.flatten().all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        this.transaction = this.transaction.copy(acceptVal = acceptVal, acceptNum = myBallotNumber)

        signal(Signal.BeforeSendingAgree, this.transaction, getPeersFromChange(enrichedChange))

        val agreedResponses = ftAgreePhase(enrichedChange, acceptVal)

        signal(Signal.BeforeSendingApply, this.transaction, getPeersFromChange(enrichedChange))

        val applyResponses = applyPhase(enrichedChange, acceptVal)
    }

    override suspend fun performProtocolAsRecoveryLeader(change: Change) {
        val electResponses = electMePhase(
            change,
            { responses -> superMajority(responses, getPeersFromChange(change)) },
            this.transaction,
            this.transaction.acceptNum
        )

        val messageWithDecision = electResponses.flatten().find { it.decision }
        if (messageWithDecision != null) {
            logger.info("${getPeerName()} Got hit with message with decision true")
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = messageWithDecision.acceptVal)
            signal(Signal.BeforeSendingAgree, this.transaction, getPeersFromChange(change))

            val agreedResponses = ftAgreePhase(
                change,
                messageWithDecision.acceptVal!!,
                decision = messageWithDecision.decision,
                acceptNum = this.transaction.acceptNum
            )

            signal(Signal.BeforeSendingApply, this.transaction, getPeersFromChange(change))

            val applyResponses = applyPhase(change, messageWithDecision.acceptVal)

            return
        }

        // I got to ft-agree phase, so my voice of this is crucial
        signal(Signal.BeforeSendingAgree, this.transaction, getPeersFromChange(change))

        logger.info("${getPeerName()} Recovery leader transaction state: ${this.transaction}")
        val agreedResponses = ftAgreePhase(change, this.transaction.acceptVal!!, acceptNum = this.transaction.acceptNum)

        signal(Signal.BeforeSendingApply, this.transaction, getPeersFromChange(change))

        val applyResponses = applyPhase(change, this.transaction.acceptVal!!)

        return
    }

    private suspend fun electMePhase(
        change: Change,
        superFunction: (List<List<ElectedYou>>) -> Boolean,
        transaction: Transaction? = null,
        acceptNum: Int? = null
    ): List<List<ElectedYou>> {
        var tries = 0
        var electResponses: List<List<ElectedYou>>

        if (!historyManagement.canBeBuild(change)) {
            throw HistoryCannotBeBuildException()
        }

        do {
            myBallotNumber++
            if (!historyManagement.canBeBuild(change)) throw HistoryCannotBeBuildException()
            this.transaction = transaction ?: Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT)

            signal(Signal.BeforeSendingElect, this.transaction, getPeersFromChange(change))
            logger.info("${getPeerName()} - sending ballot number: $myBallotNumber")
            val (responses, maxBallotNumber) = getElectedYouResponses(change, getPeersFromChange(change), acceptNum)

            electResponses = responses
            tries++
            if (superFunction(electResponses)) {
                return electResponses
            }
            myBallotNumber = max(maxBallotNumber, myBallotNumber)
            logger.info("${getPeerName()} Bumped ballot number to: $myBallotNumber")
        } while (tries < maxLeaderElectionTries)

        transactionBlocker.releaseBlock()
        throw MaxTriesExceededException()
    }

    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> {
        transactionBlocker.tryToBlock()

        val agreedResponses = getAgreedResponses(change, getPeersFromChange(change), acceptVal, decision, acceptNum)
        if (!superSet(agreedResponses, getPeersFromChange(change))) throw TooFewResponsesException()

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

    private fun signal(signal: Signal, transaction: Transaction?, peers: List<List<String>>) {
        signalPublisher.signal(signal, this, peers, transaction)
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

    private fun getPeersFromChange(change: Change): List<List<String>> {
        return change.peers.map { peer ->
            if (peer == myAddress) return@map allPeers[myPeersetId]!!
            allPeers.values.find { it.contains(peer) }
                ?: throw PeerNotInPeersetException(peer)
        }
    }

    override fun setPeers(peers: Map<Int, List<String>>) {
        this.allPeers = peers
    }

    override fun setMyAddress(address: String) {
        this.myAddress = address
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }

    override fun getPeerName() = "peerset${myPeersetId}/peer${myNodeId}"
}
