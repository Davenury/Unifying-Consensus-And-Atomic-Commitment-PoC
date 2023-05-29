package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import com.github.davenury.ucac.raftHttpClient
import com.zopa.ktor.opentracing.asyncTraced
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>>


    suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>,
    ): List<RaftResponse<ConsensusHeartbeatResponse?>>

    suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
    ): RaftResponse<ConsensusHeartbeatResponse?>


    suspend fun sendRequestApplyChange(
        address: String,
        change: Change
    ): ChangeResult
}

class RaftProtocolClientImpl(private val peersetId: PeersetId) : RaftProtocolClient {
    override suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>> {
        logger.debug("Sending elect me requests to ${otherPeers.map { it.peerId }}")
        return otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }
    }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>
    ): List<RaftResponse<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")

    override suspend fun sendConsensusHeartbeat(
        peer: PeerAddress, message: ConsensusHeartbeat,
    ): RaftResponse<ConsensusHeartbeatResponse?> {

        return CoroutineScope(Dispatchers.IO).asyncTraced(MDCContext()) {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(peer, "consensus/heartbeat", message)
        }.let {
            val result = try {
                it.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${peer.peerId}", e)
                null
            }
            RaftResponse(peer.address, result)
        }
    }

    override suspend fun sendRequestApplyChange(address: String, change: Change) =
        httpClient.post<ChangeResult>("http://${address}/consensus/request_apply_change?peerset=${peersetId}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<PeerAddress, T>>,
        urlPath: String,
    ): List<RaftResponse<K?>> =
        peersWithBody.map {
            val peer = it.first
            val body = it.second
            CoroutineScope(Dispatchers.IO).asyncTraced(MDCContext()) {
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
        logger.debug("Sending request to: ${peer.peerId}, message: $message")
        return raftHttpClient.post<Response>("http://${peer.address}/${suffix}?peerset=${peersetId}") {
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
