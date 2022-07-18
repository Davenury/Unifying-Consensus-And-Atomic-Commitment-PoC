package com.example.gpac.domain

import com.example.AdditionalAction
import com.example.EventPublisher
import com.example.SignalSubject
import com.example.TestAddon
import com.example.common.*
import org.slf4j.LoggerFactory
import kotlin.math.max

interface GPACProtocol : SignalSubject {
    suspend fun handleElect(message: ElectMe): ElectedYou
    suspend fun handleAgree(message: Agree): Agreed
    suspend fun handleApply(message: Apply)
    suspend fun performProtocolAsLeader(change: ChangeDto)
    suspend fun performProtocolAsRecoveryLeader(change: ChangeDto)
    fun getTransaction(): Transaction
    fun getBallotNumber(): Int
}

class GPACProtocolImpl(
    private val historyManagement: HistoryManagement,
    private val maxLeaderElectionTries: Int,
    private val timer: ProtocolTimer,
    private val protocolClient: ProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    private val otherPeers: List<List<String>>,
    private val addons: Map<TestAddon, AdditionalAction> = emptyMap(),
    private val eventPublisher: EventPublisher = EventPublisher(emptyList()),
    private val me: Int,
    private val myPeersetId: Int
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
            state?.find { it.acceptNum == acceptNum }
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

        signal(TestAddon.OnHandlingElectBegin, null)

        transactionBlocker.assertICanSendElectedYou()

        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(myBallotNumber, message.ballotNumber)
        val initVal = if (historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT

        transaction = Transaction(ballotNumber = message.ballotNumber, initVal = initVal)

        signal(TestAddon.OnHandlingElectEnd, transaction)

        return ElectedYou(
            message.ballotNumber,
            initVal,
            transaction.acceptNum,
            transaction.acceptVal,
            transaction.decision
        )
    }

    override suspend fun handleAgree(message: Agree): Agreed {

        signal(TestAddon.OnHandlingAgreeBegin, transaction)

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

        logger.info("$me state transaction state: ${this.transaction}")

        myBallotNumber = message.ballotNumber

        if (!message.decision) {
            transactionBlocker.tryToBlock()
            logger.info("$me Lock aquired: ${message.ballotNumber}")
        }

        signal(TestAddon.OnHandlingAgreeEnd, transaction)

        leaderFailTimeoutStart(message.change)

        return Agreed(transaction.ballotNumber, message.acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        leaderFailTimeoutStop()
        signal(TestAddon.OnHandlingApplyBegin, transaction)

        try {
            this.transaction =
                this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)

            logger.info("$me - my state: ${historyManagement.getState()}")

            if (message.acceptVal == Accept.COMMIT && !transactionWasAppliedBefore()) {
                historyManagement.change(message.change.toChange(), this.transaction.acceptNum)
            }
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT)

            logger.info("$me Releasing semaphore as cohort")
            transactionBlocker.releaseBlock()

            signal(TestAddon.OnHandlingApplyEnd, transaction)
        }
    }

    private fun transactionWasAppliedBefore() =
        historyManagement.getState()?.any { it.acceptNum == this.transaction.acceptNum } == true

    private suspend fun leaderFailTimeoutStart(change: ChangeDto) {
        logger.info("$me Start counting")
        timer.startCounting {
            logger.info("$me Recovery leader starts")
            transactionBlocker.releaseBlock()
            performProtocolAsRecoveryLeader(change)
        }
    }

    private fun leaderFailTimeoutStop() {
        logger.info("$me Stop counter")
        timer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: ChangeDto
    ) {
        val electResponses = electMePhase(change, { responses -> superSet(responses) })

        val acceptVal =
            if (electResponses.flatten().all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        this.transaction = this.transaction.copy(acceptVal = acceptVal, acceptNum = myBallotNumber)

        signal(TestAddon.BeforeSendingAgree, this.transaction)

        val agreedResponses = ftAgreePhase(change, acceptVal)

        signal(TestAddon.BeforeSendingApply, this.transaction)

        val applyResponses = applyPhase(change, acceptVal)
    }

    override suspend fun performProtocolAsRecoveryLeader(change: ChangeDto) {
        val electResponses = electMePhase(
            change,
            { responses -> superMajority(responses) },
            this.transaction,
            this.transaction.acceptNum
        )

        val messageWithDecision = electResponses.flatten().find { it.decision }
        if (messageWithDecision != null) {
            logger.info("$me Got hit with message with decision true")
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = messageWithDecision.acceptVal)
            signal(TestAddon.BeforeSendingAgree, this.transaction)

            val agreedResponses = ftAgreePhase(
                change,
                messageWithDecision.acceptVal!!,
                decision = messageWithDecision.decision,
                acceptNum = this.transaction.acceptNum
            )

            signal(TestAddon.BeforeSendingApply, this.transaction)

            val applyResponses = applyPhase(change, messageWithDecision.acceptVal)

            return
        }

        // I got to ft-agree phase, so my voice of this is crucial
        signal(TestAddon.BeforeSendingAgree, this.transaction)

        logger.info("$me Recovery leader transaction state: ${this.transaction}")
        val agreedResponses = ftAgreePhase(change, this.transaction.acceptVal!!, acceptNum = this.transaction.acceptNum)

        signal(TestAddon.BeforeSendingApply, this.transaction)

        val applyResponses = applyPhase(change, this.transaction.acceptVal!!)

        return
    }

    private suspend fun electMePhase(
        change: ChangeDto,
        superFunction: (List<List<ElectedYou>>) -> Boolean,
        transaction: Transaction? = null,
        acceptNum: Int? = null
    ): List<List<ElectedYou>> {
        var tries = 0
        var electResponses: List<List<ElectedYou>>

        if (!historyManagement.canBeBuild(change.toChange())) {
            throw HistoryCannotBeBuildException()
        }

        do {
            myBallotNumber++
            if (!historyManagement.canBeBuild(change.toChange())) throw HistoryCannotBeBuildException()
            this.transaction = transaction ?: Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT)
            signal(TestAddon.BeforeSendingElect, this.transaction)
            logger.info("$me - sending ballot number: $myBallotNumber")
            val (responses, maxBallotNumber) = getElectedYouResponses(change, otherPeers, acceptNum)
            electResponses = responses
            tries++
            if (superFunction(electResponses)) {
                return electResponses
            }
            myBallotNumber = max(maxBallotNumber, myBallotNumber)
            logger.info("$me Bumped ballot number to: $myBallotNumber")
        } while (tries < maxLeaderElectionTries)

        transactionBlocker.releaseBlock()
        throw MaxTriesExceededException()
    }

    private suspend fun ftAgreePhase(
        change: ChangeDto,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> {
        transactionBlocker.tryToBlock()

        val agreedResponses = getAgreedResponses(change, otherPeers, acceptVal, decision, acceptNum)
        if (!superSet(agreedResponses)) throw TooFewResponsesException()

        this.transaction = this.transaction.copy(decision = true)
        return agreedResponses
    }

    private suspend fun applyPhase(change: ChangeDto, acceptVal: Accept): List<Int> {
        val applyMessages = sendApplyMessages(change, otherPeers, acceptVal)
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
        change: ChangeDto,
        otherPeers: List<List<String>>,
        acceptNum: Int? = null
    ): ResponsesWithErrorAggregation<ElectedYou> =
        protocolClient.sendElectMe(
            otherPeers.filterIndexed { index, _ -> isInTransaction(index) }, ElectMe(myBallotNumber, change, acceptNum)
        )

    private suspend fun getAgreedResponses(
        change: ChangeDto,
        otherPeers: List<List<String>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null
    ): List<List<Agreed>> =
        protocolClient.sendFTAgree(
            otherPeers.filterIndexed { index, _ -> isInTransaction(index) },
            Agree(myBallotNumber, acceptVal, change, decision, acceptNum)
        )

    private suspend fun sendApplyMessages(change: ChangeDto, otherPeers: List<List<String>>, acceptVal: Accept) =
        protocolClient.sendApply(
            otherPeers.filterIndexed { index, _ -> isInTransaction(index) }, Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change
            )
        )

    private fun isInTransaction(index: Int) = true

    private suspend fun signal(addon: TestAddon, transaction: Transaction?) {
        eventPublisher.signal(addon, this)
        addons[addon]?.invoke(transaction)
    }

    private fun <T> superMajority(responses: List<List<T>>): Boolean =
        superFunction(responses, 2)

    private fun <T> superSet(responses: List<List<T>>): Boolean =
        superFunction(responses, 1)

    private fun <T> superFunction(responses: List<List<T>>, divider: Int): Boolean {
        val peersInTransaction = otherPeers.filterIndexed { index, _ -> isInTransaction(index) }
        val allShards = peersInTransaction.size >= responses.size / divider.toDouble()

        return responses.withIndex()
            .all { (index, value) ->
                val positiveResponses = if (index + 1 == myPeersetId) value.size + 1 else value.size
                val allPeers = if (index + 1 == myPeersetId) peersInTransaction[index].size + 1 else peersInTransaction[index].size
                positiveResponses > allPeers / 2F
            } && allShards
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }
}
