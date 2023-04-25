package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.Change
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import com.zopa.ktor.opentracing.asyncTraced
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

interface TwoPCProtocolClient {
    suspend fun sendAccept(peers: Map<PeersetId, PeerAddress>, change: Change): List<Boolean>
    suspend fun sendDecision(peers: Map<PeersetId, PeerAddress>, decisionChange: Change): List<Boolean>

    suspend fun askForChangeStatus(peer: PeerAddress, change: Change, otherPeerset: PeersetId): Change?
}

class TwoPCProtocolClientImpl : TwoPCProtocolClient {

    override suspend fun sendAccept(peers: Map<PeersetId, PeerAddress>, change: Change): List<Boolean> =
        sendMessages(peers, change, "2pc/accept")


    override suspend fun sendDecision(peers: Map<PeersetId, PeerAddress>, decisionChange: Change): List<Boolean> =
        sendMessages(peers, decisionChange, "2pc/decision")

    override suspend fun askForChangeStatus(peer: PeerAddress, change: Change, peersetId: PeersetId): Change? {
        val url = "http://${peer.address}/2pc/ask/${change.id}?peerset=${peersetId}"
        logger.info("Sending to: $url")
        return try {
            httpClient.get<Change?>(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }
        } catch (e: Exception) {
            logger.error("Error while evaluating response from ${peer}: $e", e)
            null
        }
    }

    private suspend fun <T> sendMessages(peers: Map<PeersetId, PeerAddress>, body: T, urlPath: String): List<Boolean> =
        peers.map { (peersetId, peerAddress) ->
            CoroutineScope(Dispatchers.IO).asyncTraced(MDCContext()) {
                send2PCMessage<T, Unit>("http://${peerAddress.address}/$urlPath?peerset=${peersetId}", body)
            }.let { coroutine ->
                Pair(peerAddress, coroutine)
            }
        }.map {
            val result = try {
                it.second.await()
                true
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${it.first}", e)
                false
            }

            result
        }

    private suspend inline fun <Message, reified Response> send2PCMessage(
        url: String,
        message: Message
    ): Response? {
        logger.info("Sending to: $url")
        return httpClient.post<Response>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc-client")
    }
}
