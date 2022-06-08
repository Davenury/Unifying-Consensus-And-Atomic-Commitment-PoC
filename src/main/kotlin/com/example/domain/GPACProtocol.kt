package com.example.domain

import com.example.AdditionalAction
import com.example.EventPublisher
import com.example.SignalSubject
import com.example.TestAddon
import io.ktor.client.statement.*
import org.slf4j.LoggerFactory

// TODO - ask if we should reject transaction or just wait

interface GPACProtocol : SignalSubject {
    suspend fun handleElect(message: ElectMe): ElectedYou
    suspend fun handleAgree(message: Agree): Agreed
    suspend fun handleApply(message: Apply)
    suspend fun performProtocolAsLeader(change: ChangeDto)
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
    private val eventPublisher: EventPublisher = EventPublisher(emptyList())
) : GPACProtocol {

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT)

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

    override suspend fun handleElect(message: ElectMe): ElectedYou {

        signal(TestAddon.OnHandlingElectBegin, null)

        if (!transactionBlocker.canISendElectedYou()) {
            throw AlreadyLockedException()
        }

        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(myBallotNumber)
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
            throw NotElectingYou(myBallotNumber)
        }
        val acceptVal = if (historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT
        this.transaction =
            this.transaction.copy(
                ballotNumber = message.ballotNumber,
                acceptVal = acceptVal,
                acceptNum = message.ballotNumber
            )

        myBallotNumber = message.ballotNumber

        transactionBlocker.tryToBlock()
        logger.info("Lock aquired: ${message.ballotNumber}")

        signal(TestAddon.OnHandlingAgreeEnd, transaction)

        selfdestruct(message.change)

        return Agreed(transaction.ballotNumber, acceptVal)
    }

    override suspend fun handleApply(message: Apply) {
        recover()
        signal(TestAddon.OnHandlingApplyBegin, transaction)

        try {
            this.transaction =
                this.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)

            if (message.acceptVal == Accept.COMMIT) {
                historyManagement.change(message.change.toChange())
            }
        } finally {
            transaction = Transaction(myBallotNumber, Accept.ABORT)

            logger.info("Releasing semaphore as cohort")
            transactionBlocker.releaseBlock()

            signal(TestAddon.OnHandlingApplyEnd, transaction)
        }
    }

    private fun selfdestruct(change: ChangeDto) {
        timer.startCounting { performProtocolAsLeader(change) }
    }
    private fun recover() {
        timer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: ChangeDto
    ) {
        var electResponses = electMePhase(change) { responses -> superMajority(responses) }

        // TODO - check for decision true
        val messageWithDecision = electResponses.flatten().find { it.decision }
        if (messageWithDecision != null) {
            // end of protocol, this cohort only needs to send apply once more to all the other peers
            val applyMessages = applyPhase(change, messageWithDecision.acceptVal!!)
            return
        }

        var acceptVal = if (electResponses.flatten().all { it.acceptVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        if (acceptVal == Accept.COMMIT) {
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = acceptVal)
            signal(TestAddon.BeforeSendingAgree, this.transaction)

            val agreedResponses = ftAgreePhase(change, acceptVal)

            signal(TestAddon.BeforeSendingApply, this.transaction)

            val applyResponses = applyPhase(change, acceptVal)

            return
        }

        if (!superSet(electResponses)) {
            logger.info("Got super majority from electing first time but not super set")
            electResponses = electMePhase(change) { responses -> superSet(responses) }
        }

        acceptVal = if (electResponses.flatten().all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT
        this.transaction = this.transaction.copy(acceptVal = acceptVal)

        signal(TestAddon.BeforeSendingAgree, this.transaction)

        val agreedResponses = ftAgreePhase(change, acceptVal)

        signal(TestAddon.BeforeSendingApply, this.transaction)

        val applyResponses = applyPhase(change, acceptVal)
    }

    private suspend fun electMePhase(change: ChangeDto, superFunction: (List<List<ElectedYou>>) -> Boolean): List<List<ElectedYou>> {
        var tries = 0
        var electResponses: List<List<ElectedYou>>

        if (!historyManagement.canBeBuild(change.toChange())) {
            throw HistoryCannotBeBuildException()
        }

        do {
            myBallotNumber++
            if (!historyManagement.canBeBuild(change.toChange())) throw HistoryCannotBeBuildException()
            this.transaction = Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT)
            signal(TestAddon.BeforeSendingElect, this.transaction)
            electResponses = getElectedYouResponses(change, otherPeers)
            tries++
        } while (!superFunction(electResponses) && tries < maxLeaderElectionTries)

        if (tries >= maxLeaderElectionTries) throw MaxTriesExceededException()

        return electResponses
    }

    private suspend fun ftAgreePhase(change: ChangeDto, acceptVal: Accept): List<List<Agreed>> {
        transactionBlocker.tryToBlock()

        val agreedResponses = getAgreedResponses(change, otherPeers, acceptVal)
        if (!superSet(agreedResponses)) throw TooFewResponsesException()

        this.transaction = this.transaction.copy(decision = true)
        return agreedResponses
    }

    private suspend fun applyPhase(change: ChangeDto, acceptVal: Accept): List<String> {
        val applyMessages = sendApplyMessages(change, otherPeers, acceptVal).flatten().map { it.receive<String>() }
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
        otherPeers: List<List<String>>
    ): List<List<ElectedYou>> =
        protocolClient.sendElectMe(
            otherPeers.filterIndexed { index, _ -> isInTransaction(index) }, ElectMe(myBallotNumber, change)
        )

    private suspend fun getAgreedResponses(
        change: ChangeDto,
        otherPeers: List<List<String>>,
        acceptVal: Accept
    ): List<List<Agreed>> =
        protocolClient.sendFTAgree(
            otherPeers.filterIndexed { index, _ -> isInTransaction(index) }, Agree(myBallotNumber, acceptVal, change)
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
        val allShards = peersInTransaction.size >= responses.size / divider

        return responses.withIndex()
            .all { (index, value) -> value.size > peersInTransaction[index].size / 2 } && allShards
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }
}
