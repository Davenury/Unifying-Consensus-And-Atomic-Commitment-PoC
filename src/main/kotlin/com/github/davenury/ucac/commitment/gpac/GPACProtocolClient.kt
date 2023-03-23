package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

interface GPACProtocolClient {
    suspend fun sendElectMe(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: ElectMe
    ): Map<PeersetId, List<Deferred<ElectedYou?>>>

    suspend fun sendFTAgree(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: Agree
    ): Map<PeersetId, List<Deferred<Agreed?>>>

    suspend fun sendApply(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: Apply
    ): Map<PeersetId, List<Deferred<HttpResponse?>>>
}

class GPACProtocolClientImpl : GPACProtocolClient {

    override suspend fun sendElectMe(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: ElectMe
    ): Map<PeersetId, List<Deferred<ElectedYou?>>> =
        sendRequests<ElectMe, ElectedYou>(
            otherPeers,
            message,
            "elect",
        ) { peer, e -> "Peer ${peer.peerId} responded with exception: $e - election" }

    override suspend fun sendFTAgree(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: Agree
    ): Map<PeersetId, List<Deferred<Agreed?>>> =
        sendRequests<Agree, Agreed>(
            otherPeers,
            message,
            "ft-agree"
        ) { peer, e -> "Peer ${peer.peerId} responded with exception: $e - ft agreement" }

    override suspend fun sendApply(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        message: Apply
    ): Map<PeersetId, List<Deferred<HttpResponse?>>> =
        sendRequests<Apply, HttpResponse>(
            otherPeers, message, "apply"
        ) { peer, e -> "Peer: ${peer.peerId} didn't apply transaction: $e" }

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (PeerAddress, Throwable) -> String
    ): Map<PeersetId, List<Deferred<K?>>> = otherPeers.mapValues { (_, peerset) ->
        peerset.map { peer ->
            CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                gpacHttpCall<K, T>(
                    "http://${peer.address}/$urlPath",
                    requestBody,
                ) { throwable -> errorMessage(peer, throwable) }
            }
        }
    }

    private suspend inline fun <reified Response, Message> gpacHttpCall(
        url: String,
        requestBody: Message,
        errorMessage: (Throwable) -> String
    ): Response? =
        try {
            logger.debug("Sending $requestBody to: $url")
            val response = httpClient.post<Response>(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = requestBody!!
            }
            response
        } catch (e: Exception) {
            logger.error(errorMessage(e), e)
            null
        }

    companion object {
        private val logger = LoggerFactory.getLogger("gpac-client")
    }
}
