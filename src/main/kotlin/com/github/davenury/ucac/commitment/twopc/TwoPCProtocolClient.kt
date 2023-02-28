package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.Change
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory

interface TwoPCProtocolClient {
    suspend fun sendAccept(peers: List<PeerAddress>, change: Change): List<Boolean>
    suspend fun sendDecision(peers: List<PeerAddress>, decisionChange: Change): List<Boolean>

    suspend fun askForChangeStatus(peer: PeerAddress, change: Change): Change?
}

class TwoPCProtocolClientImpl(private val id: Int) : TwoPCProtocolClient {


    override suspend fun sendAccept(peers: List<PeerAddress>, change: Change): List<Boolean> =
        sendMessages(peers, change, "2pc/accept")


    override suspend fun sendDecision(peers: List<PeerAddress>, decisionChange: Change): List<Boolean> =
        sendMessages(peers, decisionChange, "2pc/decision")

    override suspend fun askForChangeStatus(peer: PeerAddress, change: Change): Change? {
        val url = "http://${peer.address}/2pc/ask/${change.id}"
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

    private suspend fun <T> sendMessages(peers: List<PeerAddress>, body: T, path: String): List<Boolean> =
        sendRequests(peers.map { Pair(it, body) }, path)

    private suspend inline fun <T> sendRequests(
        peersWithBody: List<Pair<PeerAddress, T>>,
        urlPath: String
    ): List<Boolean> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async {
                send2PCMessage<T, Unit>("http://${it.first.address}/$urlPath", it.second)
            }.let { coroutine ->
                Pair(it.first, coroutine)
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
        logger.info("$id - Sending to: $url")
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
