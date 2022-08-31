package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.consensus.raft.domain.*
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(otherPeers: List<String>, message: ConsensusElectMe): List<Response<ConsensusElectedYou?>>
    suspend fun sendConsensusImTheLeader(otherPeers: List<String>, message: ConsensusImTheLeader): List<Response<String?>>
    suspend fun sendConsensusHeartbeat(peersWithMessage: List<Pair<String, ConsensusHeartbeat>>): List<Response<ConsensusHeartbeatResponse?>>
}

class RaftProtocolClientImpl : RaftProtocolClient {

    override suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<Response<ConsensusElectedYou?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }

    override suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<Response<String?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/leader") }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<String, ConsensusHeartbeat>>
    ): List<Response<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")


    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<String, T>>,
        urlPath: String
    ): List<Response<K?>> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async {
                sendConsensusMessage<T, K>("http://${it.first}/$urlPath", it.second)
            }.let { corutine ->
                Pair(it.first, corutine)
            }
        }.map {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating responses: $e")
                null
            }

            Response(it.first, result)
        }


    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        url: String,
        message: Message
    ): Response? = try {
        logger.info("Sending to: $url")
        httpClient.post<Response>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    } catch (e: Exception) {
        logger.error("Request to $url ends with: $e")
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftProtocolClientImpl::class.java)
    }
}

data class Response<K>(val from: String, val message: K)