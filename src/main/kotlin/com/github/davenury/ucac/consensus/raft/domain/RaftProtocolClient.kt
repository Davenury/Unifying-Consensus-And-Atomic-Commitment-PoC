package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.AbstractProtocolClient
import com.github.davenury.ucac.consensus.raft.domain.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(otherPeers: List<String>, message: ConsensusElectMe): List<ConsensusElectedYou?>
    suspend fun sendConsensusImTheLeader(otherPeers: List<String>, message: ConsensusImTheLeader): List<String?>
    suspend fun sendConsensusHeartbeat(peersWithMessage: List<Pair<String, ConsensusHeartbeat>>): List<ConsensusHeartbeatResponse?>
}

class RaftProtocolClientImpl : RaftProtocolClient, AbstractProtocolClient() {

    override suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<ConsensusElectedYou?> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }

    override suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<String?> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/leader") }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<String, ConsensusHeartbeat>>
    ): List<ConsensusHeartbeatResponse?> =
        sendRequests(peersWithMessage,"consensus/heartbeat")


    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<String, T>>,
        urlPath: String
    ): List<K?> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async {
                sendConsensusMessage<T, K>("http://${it.first}/$urlPath", it.second)
            }
        }.map { job ->
            try {
                job.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating responses: $e")
                null
            }
        }


    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        url: String,
        message: Message
    ): Response? = try {
        httpCall<Message, Response>(url, message)
    } catch (e: Exception) {
        logger.error("Request to $url ends with: $e")
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftProtocolClientImpl::class.java)
    }


}