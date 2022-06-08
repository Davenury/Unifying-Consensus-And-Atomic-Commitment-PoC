package com.example.domain

import com.example.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

interface ProtocolClient {
    suspend fun sendElectMe(otherPeers: List<List<String>>, message: ElectMe): List<List<ElectedYou>>
    suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>>
    suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<List<HttpStatement>>
}

class ProtocolClientImpl : ProtocolClient {

    override suspend fun sendElectMe(otherPeers: List<List<String>>, message: ElectMe): List<List<ElectedYou>> =
        sendRequests(
            otherPeers,
            message,
            "elect"
        ) { singlePeer, e -> "Peer $singlePeer responded with exception: $e - election" }

    override suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>> =
        sendRequests(
            otherPeers,
            message,
            "ft-agree"
        ) { singlePeer, e -> "Peer $singlePeer responded with exception: $e - ft agreement" }

    override suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<List<HttpStatement>> =
        sendRequests(
            otherPeers, message, "apply"
        ) { it, e -> "Peer: $it didn't apply transaction: $e" }

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: List<List<String>>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (String, Throwable) -> String
    ) =
        otherPeers.map { peersets ->
            peersets.mapNotNull {
                withContext(Dispatchers.IO) {
                    try {
                        val url = "http://$it/$urlPath"
                        logger.info("Sending to: $url")
                        httpClient.post<K>(url) {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body = requestBody!!
                        }
                    } catch (e: Exception) {
                        logger.error(errorMessage(it, e))
                        null
                    }
                }
            }.also {
                logger.info("Got responses: $it")
            }
        }

    companion object {
        private val logger = LoggerFactory.getLogger(ProtocolClientImpl::class.java)
    }
}