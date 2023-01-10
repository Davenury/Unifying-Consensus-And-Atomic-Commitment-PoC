package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.PeerAddress
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
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>>

    suspend fun sendConsensusImTheLeader(
        otherPeers: List<PeerAddress>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>>

    suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>,
    ): List<RaftResponse<ConsensusHeartbeatResponse?>>

    suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
    ): RaftResponse<ConsensusHeartbeatResponse?>
}

class RaftProtocolClientImpl : RaftProtocolClient {

    override suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>> {
        logger.debug("Sending elect me requests to ${otherPeers.map { it.globalPeerId }}")
        return otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }
    }

    override suspend fun sendConsensusImTheLeader(
        otherPeers: List<PeerAddress>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/leader") }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>
    ): List<RaftResponse<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")

    override suspend fun sendConsensusHeartbeat(
        peer: PeerAddress, message: ConsensusHeartbeat,
    ): RaftResponse<ConsensusHeartbeatResponse?> {

        return CoroutineScope(Dispatchers.IO).async(MDCContext()) {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(peer, "consensus/heartbeat", message)
        }.let {
            val result = try {
                it.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${peer.globalPeerId}", e)
                null
            }
            RaftResponse(peer.address, result)
        }
    }

    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<PeerAddress, T>>,
        urlPath: String,
    ): List<RaftResponse<K?>> =
        peersWithBody.map {
            val peer = it.first
            val body = it.second
            CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                sendConsensusMessage<T, K>(peer, urlPath, body)
            }.let { coroutine ->
                Pair(peer, coroutine)
            }
        }.map {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${it.first}", e)
                null
            }

            RaftResponse(it.first.address, result)
        }

    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        peer: PeerAddress,
        suffix: String,
        message: Message,
    ): Response? {
        logger.debug("Sending request to: ${peer.globalPeerId}, message: $message")
        return raftHttpClient().use { client ->
            client.post<Response>("http://${peer.address}/${suffix}") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = message!!
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("raft-client")
    }
}

data class RaftResponse<K>(val from: String, val message: K)
