package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>>

    suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>>

    suspend fun sendConsensusHeartbeat(peersWithMessage: List<Pair<String, ConsensusHeartbeat>>): List<RaftResponse<ConsensusHeartbeatResponse?>>
    suspend fun sendConsensusHeartbeat(peerWithMessage: Pair<String, ConsensusHeartbeat>): RaftResponse<ConsensusHeartbeatResponse?>
}

class RaftProtocolClientImpl : RaftProtocolClient {

    override suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>> {
        logger.info("Sending elect me requests to $otherPeers")
        return otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }
    }

    override suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/leader") }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<String, ConsensusHeartbeat>>
    ): List<RaftResponse<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")

    override suspend fun sendConsensusHeartbeat(
        peerWithMessage: Pair<String, ConsensusHeartbeat>
    ): RaftResponse<ConsensusHeartbeatResponse?> =
        CoroutineScope(Dispatchers.IO).async(MDCContext()) {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(
                "http://${peerWithMessage.first}/consensus/heartbeat",
                peerWithMessage.second
            )
        }.let {
            Pair(peerWithMessage.first, it)
        }.let {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${it.first}", e)
                null
            }
            RaftResponse(it.first, result)
        }

    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<String, T>>,
        urlPath: String
    ): List<RaftResponse<K?>> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                sendConsensusMessage<T, K>("http://${it.first}/$urlPath", it.second)
            }.let { coroutine ->
                Pair(it.first, coroutine)
            }
        }.map {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${it.first}", e)
                null
            }

            RaftResponse(it.first, result)
        }

    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        url: String,
        message: Message
    ): Response? {
        logger.debug("Sending request to: $url, message: $message")
        return raftHttpClient.post<Response>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("raft-client")
    }
}

data class RaftResponse<K>(val from: String, val message: K)
