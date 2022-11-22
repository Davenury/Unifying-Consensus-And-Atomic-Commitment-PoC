package com.github.davenury.ucac.commitment.TwoPC

import com.github.davenury.common.Change
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory

data class ResponsesWithErrorAggregation<K>(
    val responses: List<List<K>>,
    val aggregatedValue: Int?
)

interface TwoPCProtocolClient {
    suspend fun sendAccept(peers: List<String>, change: Change): List<Boolean>
    suspend fun sendDecision(peers: List<String>, decisionChange: Change): List<Boolean>
}

class TwoPCProtocolClientImpl(private val id: Int) : TwoPCProtocolClient {


    override suspend fun sendAccept(peers: List<String>, change: Change): List<Boolean> =
        sendMessages(peers, change, "2pc/accept")


    override suspend fun sendDecision(peers: List<String>, decisionChange: Change): List<Boolean> =
        sendMessages(peers, decisionChange, "2pc/decision")


    private suspend fun <T> sendMessages(peers: List<String>, body: T, path: String) =
        sendRequests(peers.map { Pair(it, body) }, path).map { it ?: false }


    private suspend inline fun <T> sendRequests(
        peersWithBody: List<Pair<String, T>>,
        urlPath: String
    ): List<Boolean?> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async {
                sendConsensusMessage<T, Unit>("http://${it.first}/$urlPath", it.second)
            }.let { coroutine ->
                Pair(it.first, coroutine)
            }
        }.map {
            val result = try {
                it.second.await()
                true
            } catch (e: Exception) {
                logger.error("$id - Error while evaluating response from ${it.first}: $e", e)
                null
            }

            result
        }


    private suspend inline fun <Message, reified Response> sendConsensusMessage(
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
        private val logger = LoggerFactory.getLogger(TwoPCProtocolClientImpl::class.java)
    }
}
