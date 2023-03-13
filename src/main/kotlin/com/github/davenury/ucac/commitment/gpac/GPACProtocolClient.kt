package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.httpClient
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

interface GPACProtocolClient {
    suspend fun sendElectMe(
        otherPeers: List<List<PeerAddress>>,
        message: ElectMe
    )

    suspend fun sendFTAgree(otherPeers: List<List<PeerAddress>>, message: Agree)
    suspend fun sendApply(otherPeers: List<List<PeerAddress>>, message: Apply)
}

class GPACProtocolClientImpl(
    private val peerResolver: PeerResolver,
) : GPACProtocolClient {

    override suspend fun sendElectMe(
        otherPeers: List<List<PeerAddress>>,
        message: ElectMe
    ) {
        sendRequests<ElectMe, ElectedYou>(
            otherPeers,
            message,
            "elect",
            "/gpac/leader/elect-response",
        ) { peer, e -> "Peer ${peer.globalPeerId} responded with exception: $e - election" }
    }

    override suspend fun sendFTAgree(otherPeers: List<List<PeerAddress>>, message: Agree) {
        sendRequests<Agree, Agreed>(
            otherPeers,
            message,
            "ft-agree",
            "/gpac/leader/agree-response"
        ) { peer, e -> "Peer ${peer.globalPeerId} responded with exception: $e - ft agreement" }
    }

    override suspend fun sendApply(otherPeers: List<List<PeerAddress>>, message: Apply) {
        sendRequests<Apply, HttpResponse>(
            otherPeers, message, "apply", "/gpac/leader/apply-response"
        ) { peer, e -> "Peer: ${peer.globalPeerId} didn't apply transaction: $e" }
    }

    private suspend inline fun <Message, reified K> sendRequests(
        otherPeers: List<List<PeerAddress>>,
        requestBody: Message,
        urlPath: String,
        gpacReturnPath: String,
        crossinline errorMessage: (PeerAddress, Throwable) -> String,
    ) {
        otherPeers.map { peerset ->
            peerset.map { peer ->
                CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                    gpacHttpCall(
                        "http://${peer.address}/$urlPath",
                        requestBody,
                        gpacReturnPath,
                    ) { throwable -> errorMessage(peer, throwable) }
                }
            }
        }.map { it.forEach { it.await() } }
    }

    private suspend inline fun <Message> gpacHttpCall(
        url: String,
        requestBody: Message,
        path: String,
        errorMessage: (Throwable) -> String,
    ) {
        try {
            val response = httpClient.post<HttpStatement>(url) {
                parameter(
                    "leader-return-address",
                    URLEncoder.encode(
                        "http://${peerResolver.currentPeerAddress().address}${path}",
                        StandardCharsets.UTF_8
                    )
                )
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = requestBody!!
            }
            logger.info("Response from $url: ${response.execute().status.value} (return address: ${peerResolver.currentPeerAddress().address}$path")
        } catch (e: Exception) {
            logger.error(errorMessage(e), e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("gpac-client")
    }
}
