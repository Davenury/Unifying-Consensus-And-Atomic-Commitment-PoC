package com.example.domain

import com.example.getOtherPeers
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

data class Transaction(
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int = 0,
    val acceptVal: Accept? = null,
    val decision: Boolean = false,
    val ended: Boolean = false
)

interface GPACProtocol {
    fun handleElect(message: ElectMe): ElectedYou
    fun handleAgree(message: Agree): Agreed
    fun handleApply(message: Apply)
    suspend fun performProtocolAsLeader(change: ChangeDto, otherPeers: List<String>)
    fun getTransaction(ballotNumber: Int): Transaction?
    fun getTransactions(): Map<Int, Transaction>
    fun getBallotNumber(): Int
}

class GPACProtocolImpl(
    private val historyManagement: HistoryManagement,
    private val maxLeaderElectionTries: Int,
    private val httpClient: HttpClient
) : GPACProtocol {

    private var myBallotNumber: Int = 0

    private var transactions = mutableMapOf<Int, Transaction>()

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > myBallotNumber

    override fun getTransaction(ballotNumber: Int): Transaction? = this.transactions[ballotNumber]

    override fun getTransactions(): Map<Int, Transaction> = transactions

    override fun getBallotNumber(): Int = myBallotNumber

    override fun handleElect(message: ElectMe): ElectedYou {
        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(myBallotNumber)
        val initVal = if (historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT

        val defaultTransaction = Transaction(ballotNumber = message.ballotNumber, initVal = initVal)
        transactions[message.ballotNumber] = defaultTransaction

        return ElectedYou(
            message.ballotNumber,
            initVal,
            defaultTransaction.acceptNum,
            defaultTransaction.acceptVal,
            defaultTransaction.decision
        )
    }

    override fun handleAgree(message: Agree): Agreed {
        if (!checkBallotNumber(message.ballotNumber)) {
            throw NotElectingYou(myBallotNumber)
        }
        val acceptVal = if (historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT
        this.transactions[message.ballotNumber] =
            this.transactions[message.ballotNumber]?.copy(
                ballotNumber = message.ballotNumber,
                acceptVal = acceptVal,
                acceptNum = message.ballotNumber
            )
                ?: throw IllegalStateException("Got agree for transaction that isn't in transactions map: ${message.ballotNumber}")

        myBallotNumber = message.ballotNumber

        // TODO: lock? - lock apply

        return Agreed(transactions[message.ballotNumber]!!.ballotNumber, acceptVal)
    }

    override fun handleApply(message: Apply) {
        this.transactions[message.ballotNumber] =
            this.transactions[message.ballotNumber]?.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)
                ?: throw IllegalStateException("Got apply for transaction that isn't in transactions map: ${message.ballotNumber}")

        if (message.acceptVal == Accept.COMMIT) {
            historyManagement.change(message.change.toChange())
        }
    }

    override suspend fun performProtocolAsLeader(change: ChangeDto, otherPeers: List<String>) {
        var tries = 0
        var electResponses: List<ElectedYou>
        do {
            if (!historyManagement.canBeBuild(change.toChange())) throw HistoryCannotBeBuildException()
            this.transactions[myBallotNumber] = Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT)
            electResponses = getElectedYouResponses(change, otherPeers)
            tries++
            myBallotNumber++
        } while (electResponses.size <= otherPeers.size / 2 && tries < maxLeaderElectionTries)

        if (tries >= maxLeaderElectionTries) throw MaxTriesExceededException()

        val acceptVal = if (electResponses.all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        val agreedResponses = getAgreedResponses(change, otherPeers, acceptVal)
        if (agreedResponses.size <= otherPeers.size / 2) throw TooFewResponsesException()

        this.transactions[myBallotNumber] = this.transactions[myBallotNumber]!!.copy(decision = true)
        sendApplyMessages(change, otherPeers, acceptVal)
        this.handleApply(
            Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transactions[myBallotNumber]!!.decision,
                acceptVal,
                change
            )
        )
    }

    private suspend fun getElectedYouResponses(change: ChangeDto, otherPeers: List<String>): List<ElectedYou> =
        sendRequests(
            otherPeers,
            ElectMe(myBallotNumber, change),
            "elect"
        ) { it, e -> "Peer $it responded with exception: $e - election" }

    private suspend fun getAgreedResponses(
        change: ChangeDto,
        otherPeers: List<String>,
        acceptVal: Accept
    ): List<Agreed> =
        sendRequests(
            otherPeers,
            Agree(myBallotNumber, acceptVal, change),
            "ft-agree"
        ) { it, e -> "Peer $it responded with exception: $e - ft agreement" }

    private suspend fun sendApplyMessages(change: ChangeDto, otherPeers: List<String>, acceptVal: Accept) {
        sendRequests<Apply, HttpStatement>(otherPeers, Apply(
            myBallotNumber,
            this@GPACProtocolImpl.transactions[myBallotNumber]!!.decision,
            acceptVal,
            change
        ), "apply") { it, e -> "Peer: $it didn't apply transaction: $e" }
    }

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: List<String>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (String, Throwable) -> String
    ) =
        otherPeers.mapNotNull {
            withContext(Dispatchers.IO) {
                try {
                    logger.info("Sending to: ${"http://$it/$urlPath"}")
                    httpClient.post<K>("http://$it/$urlPath") {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = requestBody!!
                    }
                } catch (e: Exception) {
                    logger.error(errorMessage(it, e))
                    null
                }
            }
        }.also {
            logger.info("Got responses: $it")
        }


    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }
}