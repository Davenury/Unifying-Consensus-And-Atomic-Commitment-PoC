package com.github.davenury.ucac.consensus.alvin

import AlvinAckPropose
import AlvinPropose
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory


//FIXME: extract shared logic for protocol clients

interface AlvinProtocolClient {

    suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?>


    suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>,
    ): List<ConsensusResponse<ConsensusHeartbeatResponse?>>

    suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
    ): ConsensusResponse<ConsensusHeartbeatResponse?>
}

class AlvinProtocolClientImpl : AlvinProtocolClient {

    override suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?> {
        logger.debug("Sending proposal request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer,message), "alvin/proposal")
    }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<PeerAddress, ConsensusHeartbeat>>
    ): List<ConsensusResponse<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")

    override suspend fun sendConsensusHeartbeat(
        peer: PeerAddress, message: ConsensusHeartbeat,
    ): ConsensusResponse<ConsensusHeartbeatResponse?> {

        return CoroutineScope(Dispatchers.IO).async(MDCContext()) {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(peer, "consensus/heartbeat", message)
        }.let {
            val result = try {
                it.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${peer.globalPeerId}", e)
                null
            }
            ConsensusResponse(peer.address, result)
        }
    }

    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<PeerAddress, T>>,
        urlPath: String,
    ): List<ConsensusResponse<K?>> =
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

            ConsensusResponse(it.first.address, result)
        }

    private suspend inline fun <T, reified K> sendRequest(
        peerWithBody: Pair<PeerAddress, T>,
        urlPath: String,
    ) = CoroutineScope(Dispatchers.IO).async(MDCContext()) {
        sendConsensusMessage<T, K>(
            peerWithBody.first,
            urlPath,
            peerWithBody.second
        )
    }.let {
        val result = try {
            it.await()
        } catch (e: Exception) {
            logger.error("Error while evaluating response from ${peerWithBody.first.globalPeerId}", e)
            null
        }
        ConsensusResponse(peerWithBody.first.address, result)
    }

    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        peer: PeerAddress,
        suffix: String,
        message: Message,
    ): Response? {
        logger.debug("Sending request to: ${peer.globalPeerId}, message: $message")
        return raftHttpClient.post<Response>("http://${peer.address}/${suffix}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("raft-client")
    }
}

data class ConsensusResponse<K>(val from: String, val message: K)
