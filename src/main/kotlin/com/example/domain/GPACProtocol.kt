package com.example.domain

import com.example.objectMapper
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.response.*
import io.ktor.http.*
import org.slf4j.LoggerFactory

data class State(val ballotNumber: Int, val initVal: Accept, val acceptNum: Int, val acceptVal: Accept?, val decision: Boolean) {
    companion object {
        fun defaultState() =
            State(0, Accept.ABORT, 0, null, false)

        fun resetStateAfterTransaction(prevState: State) =
            State(prevState.ballotNumber, Accept.ABORT, 0, null, false)
    }
}

interface GPACProtocol {
    fun handleElect(message: ElectMe): ElectedYou
    fun handleAgree(message: Agree): Agreed
    fun handleApply(message: Apply)
    suspend fun performProtocolAsLeader(change: ChangeDto, otherPeers: List<String>)
    fun getState(): State
}

class GPACProtocolImpl(
    private val historyManagement: HistoryManagement,
    private val maxLeaderElectionTries: Int,
    private val httpClient: HttpClient
): GPACProtocol {
    private var state = State.defaultState()

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > state.ballotNumber

    override fun getState(): State = this.state

    override fun handleElect(message: ElectMe): ElectedYou {
        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(this.state.ballotNumber)
        val initVal = if(historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT

        return ElectedYou(state.ballotNumber, initVal, state.acceptNum, state.acceptVal, state.decision)
    }

    override fun handleAgree(message: Agree): Agreed {
        if(checkBallotNumber(message.ballotNumber)) {
            val acceptVal = if(historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT
            this.state = this.state.copy(ballotNumber = message.ballotNumber, acceptVal = acceptVal)

            // TODO: lock?

            return Agreed(state.ballotNumber, acceptVal)
        }
        throw NotElectingYou(this.state.ballotNumber)
    }

    override fun handleApply(message: Apply) {
        this.state = this.state.copy(decision = true, acceptVal = Accept.COMMIT)
        historyManagement.change(message.change.toChange())
        this.state = State.resetStateAfterTransaction(this.state)
    }

    override suspend fun performProtocolAsLeader(change: ChangeDto, otherPeers: List<String>) {
        var tries = 0
        var electResponses: List<ElectedYou>
        do {
            this.state = this.state.copy(ballotNumber = this.state.ballotNumber + 1)
            electResponses = getElectedYouResponses(change, otherPeers)
            tries++
        } while (electResponses.size < otherPeers.size / 2 + 1 && tries < maxLeaderElectionTries)

        if (tries >= maxLeaderElectionTries) throw MaxTriesExceededException()

        val acceptVal = if (electResponses.all { it.initVal == Accept.COMMIT }) Accept.COMMIT else Accept.ABORT

        val agreedResponses = getAgreedResponses(change, otherPeers, acceptVal)
        if (agreedResponses.size < otherPeers.size / 2) throw TooFewResponsesException()

        this.state = this.state.copy(decision = true)
        sendApplyMessages(change, otherPeers)
        this.handleApply(Apply(this@GPACProtocolImpl.state.ballotNumber, this@GPACProtocolImpl.state.decision, change))
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
                    body = ElectMe(this@GPACProtocolImpl.state.ballotNumber, change)
                }
            } catch (e: Exception) {
                logger.error("Peer $it responded with exception: $e - election")
                null
            }
        }

    private suspend fun getAgreedResponses(change: ChangeDto, otherPeers: List<String>, acceptVal: Accept): List<Agreed> =
        otherPeers.mapNotNull {
            try {
                httpClient.post<Agreed>("http://$it/ft-agree") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(this@GPACProtocolImpl.state.ballotNumber, acceptVal, change)
                }
            } catch (e: Exception) {
                logger.error("Peer $it responded with exception: $e - ft agreement")
                null
            }
        }

    private suspend fun sendApplyMessages(change: ChangeDto, otherPeers: List<String>) {
        otherPeers.forEach {
            try {
                httpClient.post("http://$it/apply") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(this@GPACProtocolImpl.state.ballotNumber, this@GPACProtocolImpl.state.decision, change)
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