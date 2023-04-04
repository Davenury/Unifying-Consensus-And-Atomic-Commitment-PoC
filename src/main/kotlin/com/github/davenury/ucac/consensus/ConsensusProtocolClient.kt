package com.github.davenury.ucac.consensus

import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

open class ConsensusProtocolClient{

    suspend inline fun <T, reified K> sendRequest(
        peerWithBody: Pair<PeerAddress, T>,
        urlPath: String,
    ): ConsensusResponse<K?> = CoroutineScope(Dispatchers.IO).async(MDCContext()) {
        sendConsensusMessage<T, K>(
            peerWithBody.first,
            urlPath,
            peerWithBody.second
        )
    }.let {
        try {
            val result = it.await()
            ConsensusResponse(peerWithBody.first.address, result)
        } catch (e: Exception) {
            when {
                e is ClientRequestException && e.response.status == HttpStatusCode.Unauthorized -> {
                    logger.error("Received unauthorized response from peer: ${peerWithBody.first.peerId}")
                    ConsensusResponse(peerWithBody.first.address, null, true)
                }

                else -> {
                    logger.error("Error while evaluating response from ${peerWithBody.first.peerId}", e)
                    ConsensusResponse(peerWithBody.first.address, null)
                }
            }
        }
    }

    suspend inline fun <Message, reified Response> sendConsensusMessage(
        peer: PeerAddress,
        suffix: String,
        message: Message,
    ): Response? {
        logger.debug("Sending request to: ${peer.peerId} ${peer.address}, message: $message")
        return raftHttpClient.post<Response>("http://${peer.address}/${suffix}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger("consensus-client")
    }
}

data class ConsensusResponse<K>(val from: String, val message: K, val unauthorized: Boolean = false)