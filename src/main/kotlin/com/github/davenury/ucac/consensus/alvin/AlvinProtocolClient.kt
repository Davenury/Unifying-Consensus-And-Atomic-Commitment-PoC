package com.github.davenury.ucac.consensus.alvin

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
    ): ConsensusResponse<AlvinCommit?>
}

class AlvinProtocolClientImpl : AlvinProtocolClient {

    override suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?> {
        logger.debug("Sending proposal request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "alvin/proposal")
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinAckAccept?> {
        logger.debug("Sending accept request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "alvin/accept")
    }

    override suspend fun sendStable(peer: PeerAddress, message: AlvinStable): ConsensusResponse<AlvinAckStable?> {
        logger.debug("Sending stable request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "alvin/stable")
    }

    override suspend fun sendPrepare(peer: PeerAddress, message: AlvinAccept): ConsensusResponse<AlvinPromise?> {
        logger.debug("Sending prepare request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "alvin/prepare")
    }

    override suspend fun sendCommit(peer: PeerAddress, message: AlvinCommit): ConsensusResponse<AlvinCommit?> {
        logger.debug("Sending commit request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "alvin/commit")
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
