package com.example.domain

import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.http.*
import org.slf4j.LoggerFactory

data class Transaction(
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int,
    val acceptVal: Accept?,
    val decision: Boolean,
    val ended: Boolean = false
) {
    companion object {
        fun defaultTransaction(ballotNumber: Int, initVal: Accept = Accept.ABORT) =
            Transaction(ballotNumber, initVal, 0, null, decision = false, ended = false)
    }
}

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

        val defaultTransaction = Transaction.defaultTransaction(message.ballotNumber, initVal)
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
        if (checkBallotNumber(message.ballotNumber)) {
            val acceptVal = if (historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT
            this.transactions[message.ballotNumber] =
                this.transactions[message.ballotNumber]?.copy(
                    ballotNumber = message.ballotNumber,
                    acceptVal = acceptVal,
                    acceptNum = message.ballotNumber
                ) ?: throw IllegalStateException("Got agree for transaction that isn't in transactions map: ${message.ballotNumber}")

            myBallotNumber = message.ballotNumber

            // TODO: lock? - lock apply

            return Agreed(transactions[message.ballotNumber]!!.ballotNumber, acceptVal)
        }
        throw NotElectingYou(myBallotNumber)
    }

    override fun handleApply(message: Apply) {
        this.transactions[message.ballotNumber] =
            this.transactions[message.ballotNumber]?.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)
                ?: throw java.lang.IllegalStateException("Got apply for transaction that isn't in transactions map: ${message.ballotNumber}")

        if (message.acceptVal == Accept.COMMIT) {
            historyManagement.change(message.change.toChange())
        }
    }

    override suspend fun performProtocolAsLeader(change: ChangeDto, otherPeers: List<String>) {
        var tries = 0
        var electResponses: List<ElectedYou>
        do {
            if (!historyManagement.canBeBuild(change.toChange())) throw HistoryCannotBeBuildException()
            this.transactions[myBallotNumber] = Transaction.defaultTransaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT)
            electResponses = getElectedYouResponses(change, otherPeers)
            tries++
            if (electResponses.size <= otherPeers.size / 2 && tries < maxLeaderElectionTries) {
                myBallotNumber++
            } else {
                break
            }
        } while (true)

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
        otherPeers.mapNotNull {
            try {
                httpClient.post<ElectedYou>("http://$it/elect") {
                    method = HttpMethod.Post
                    headers {
                        append("Content-Type", "application/json")
                        append("Accept", "application/json")
                    }
                    body = ElectMe(myBallotNumber, change)
                }
            } catch (e: Exception) {
                logger.error("Peer $it responded with exception: $e - election")
                null
            }
        }

    private suspend fun getAgreedResponses(
        change: ChangeDto,
        otherPeers: List<String>,
        acceptVal: Accept
    ): List<Agreed> =
        otherPeers.mapNotNull {
            try {
                httpClient.post<Agreed>("http://$it/ft-agree") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(myBallotNumber, acceptVal, change)
                }
            } catch (e: Exception) {
                logger.error("Peer $it responded with exception: $e - ft agreement")
                null
            }
        }

    private suspend fun sendApplyMessages(change: ChangeDto, otherPeers: List<String>, acceptVal: Accept) {
        otherPeers.forEach {
            try {
                httpClient.post("http://$it/apply") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        myBallotNumber,
                        this@GPACProtocolImpl.transactions[myBallotNumber]!!.decision,
                        acceptVal,
                        change
                    )
                }
            } catch (e: Exception) {
                logger.error("Peer: $it didn't apply transaction with change: $change")
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GPACProtocolImpl::class.java)
    }
}