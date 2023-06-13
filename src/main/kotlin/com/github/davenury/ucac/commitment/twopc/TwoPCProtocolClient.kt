package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.Change
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.common.*
import com.github.davenury.ucac.httpClient
import com.zopa.ktor.opentracing.asyncTraced
import io.ktor.client.call.*
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import java.io.IOException

data class TwoPCRequestResponse(
    val success: Boolean,
    val redirect: Boolean = false,
    val failureBecauseOfDeadPeer: Boolean = false,
    val newConsensusLeaderId: PeerId? = null,
    val newConsensusLeaderPeersetId: PeersetId? = null,
    val peersetId: PeersetId,
)

interface TwoPCProtocolClient {
    suspend fun sendAccept(peers: Map<PeersetId, PeerAddress>, change: Change): Map<PeerAddress, TwoPCRequestResponse>
    suspend fun sendDecision(peers: Map<PeersetId, PeerAddress>, decisionChange: Change): Map<PeerAddress, TwoPCRequestResponse>

    suspend fun askForChangeStatus(peer: PeerAddress, change: Change, otherPeerset: PeersetId): Change?
}

class TwoPCProtocolClientImpl(
    private val myPeersetId: PeersetId,
) : TwoPCProtocolClient {

    override suspend fun sendAccept(peers: Map<PeersetId, PeerAddress>, change: Change): Map<PeerAddress, TwoPCRequestResponse> =
        sendMessages(peers, change, "2pc/accept")


    override suspend fun sendDecision(peers: Map<PeersetId, PeerAddress>, decisionChange: Change): Map<PeerAddress, TwoPCRequestResponse> =
        sendMessages(peers, decisionChange, "2pc/decision")

    override suspend fun askForChangeStatus(peer: PeerAddress, change: Change, peersetId: PeersetId): Change? {
        val url = "http://${peer.address}/2pc/ask/${change.id}?peerset=${peersetId}&sender=${myPeersetId.peersetId}"
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

    private suspend fun <T> sendMessages(peers: Map<PeersetId, PeerAddress>, body: T, urlPath: String): Map<PeerAddress, TwoPCRequestResponse> =
        peers.map { (peersetId, peerAddress) ->
            CoroutineScope(Dispatchers.IO).asyncTraced(MDCContext()) {
                send2PCMessage<T, Unit>("http://${peerAddress.address}/$urlPath?peerset=${peersetId}", body)
            }.let { coroutine ->
                Triple(peerAddress, peersetId, coroutine)
            }
        }.associate {
            val result = try {
                it.third.await()
                it.first to TwoPCRequestResponse(
                    success = true,
                    peersetId = it.second
                )
            } catch (e: RedirectResponseException) {
                logger.info("Peer ${it.first} responded with redirect")
                val newConsensusLeaderId = e.response.receive<CurrentLeaderFullInfoDto>()
                it.first to TwoPCRequestResponse(
                    success = false,
                    redirect = true,
                    newConsensusLeaderId = newConsensusLeaderId.peerId,
                    newConsensusLeaderPeersetId = newConsensusLeaderId.peersetId,
                    peersetId = it.second,
                )
            } catch(e: IOException) {
                logger.error("Error while evaluating response from ${it.first} - peer is dead", e)
                it.first to TwoPCRequestResponse(
                    success = false,
                    peersetId = it.second,
                    failureBecauseOfDeadPeer = true,
                )
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${it.first}", e)
                it.first to TwoPCRequestResponse(
                    success = false,
                    peersetId = it.second,
                )
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
