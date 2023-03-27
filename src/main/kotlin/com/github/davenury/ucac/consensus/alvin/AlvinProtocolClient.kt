package com.github.davenury.ucac.consensus.alvin

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


//FIXME: extract shared logic for protocol clients

interface AlvinProtocolClient {

    suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?>

    suspend fun sendAccept(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinAckAccept?>

    suspend fun sendStable(
        peer: PeerAddress,
        message: AlvinStable,
    ): ConsensusResponse<AlvinAckStable?>

    suspend fun sendPrepare(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinPromise?>


    suspend fun sendCommit(
        peer: PeerAddress,
        message: AlvinCommit
    ): ConsensusResponse<String?>

    suspend fun sendFastRecovery(
        peer: PeerAddress,
        message: AlvinFastRecovery
    ): ConsensusResponse<AlvinFastRecoveryResponse?>
}

public class AlvinProtocolClientImpl : AlvinProtocolClient {

    override suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?> {
        logger.debug("Sending proposal request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/proposal")
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinAckAccept?> {
        logger.debug("Sending accept request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/accept")
    }

    override suspend fun sendStable(peer: PeerAddress, message: AlvinStable): ConsensusResponse<AlvinAckStable?> {
        logger.debug("Sending stable request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/stable")
    }

    override suspend fun sendPrepare(peer: PeerAddress, message: AlvinAccept): ConsensusResponse<AlvinPromise?> {
        logger.debug("Sending prepare request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/prepare")
    }

    override suspend fun sendCommit(peer: PeerAddress, message: AlvinCommit): ConsensusResponse<String?> {
        logger.debug("Sending commit request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/commit")
    }

    override suspend fun sendFastRecovery(
        peer: PeerAddress,
        message: AlvinFastRecovery
    ): ConsensusResponse<AlvinFastRecoveryResponse?> {
        logger.debug("Sending fastRecovery request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/fast-recovery")
    }

    private suspend inline fun <T, reified K> sendRequest(
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
                    ConsensusResponse(peerWithBody.first.address, null, true)
                }

                else -> {
                    logger.error("Error while evaluating response from ${peerWithBody.first.peerId}", e)
                    ConsensusResponse(peerWithBody.first.address, null)
                }
            }
        }
    }

    private suspend inline fun <Message, reified Response> sendConsensusMessage(
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
        private val logger = LoggerFactory.getLogger("alvin-client")
    }
}

data class ConsensusResponse<K>(val from: String, val message: K, val unauthorized: Boolean = false)
