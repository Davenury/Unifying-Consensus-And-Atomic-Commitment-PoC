package com.example.domain

import com.example.httpClient
import com.example.objectMapper
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.lang.Integer.max

data class ResponsesWithErrorAggregation<K>(
    val responses: List<List<K>>,
    val aggregatedValue: Int
)

interface ProtocolClient {
    suspend fun sendElectMe(otherPeers: List<List<String>>, message: ElectMe): ResponsesWithErrorAggregation<ElectedYou>
    suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>>
    suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<List<HttpStatement>>
}

class ProtocolClientImpl : ProtocolClient {

    override suspend fun sendElectMe(otherPeers: List<List<String>>, message: ElectMe): ResponsesWithErrorAggregation<ElectedYou> =
        sendRequests<ElectMe, ElectedYou>(
            otherPeers,
            message,
            "elect",
            { singlePeer, e -> "Peer $singlePeer responded with exception: $e - election" },
            { acc, value -> max(acc, value) }
        )

    override suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>> =
        sendRequests<Agree, Agreed>(
            otherPeers,
            message,
            "ft-agree",
            { singlePeer, e -> "Peer $singlePeer responded with exception: $e - ft agreement" }
        ).responses

    override suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<List<HttpStatement>> =
        sendRequests<Apply, HttpStatement>(
            otherPeers, message, "apply",
            { it, e -> "Peer: $it didn't apply transaction: $e" }
        ).responses

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: List<List<String>>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (String, Throwable) -> String,
        crossinline aggregateErrors: (acc: Int, value: Int) -> Int = { _: Int, _: Int -> 0 }
    ): ResponsesWithErrorAggregation<K> {
        var acc = 0
        val responses: List<List<K>> = otherPeers.map { peersets ->
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
                    } catch (e: ClientRequestException) {
                        // since we're updating ballot number in electing phase, this mechanism lets us
                        // get any aggregation from all responses from "Not electing you" response, so we can get
                        // max of all ballotNumbers sent back to the leader
                        logger.error(errorMessage(it, e))
                        if (e.response.status.value == 422) {
                            val value = e.response.content.readUTF8Line()?.let {
                                return@let Regex("[0-9]+").findAll(it)
                                    .map(MatchResult::value)
                                    .toList()
                                    .map { it.toInt() }
                            }?.get(0) ?: 0
                            acc = aggregateErrors(acc, value)
                        }
                        null
                    } catch (e: Exception) {
                        logger.error(errorMessage(it, e))
                        null
                    }
                }
            }.also {
                logger.info("Got responses: $it")
            }
        }
        return ResponsesWithErrorAggregation(
            responses,
            acc
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ProtocolClientImpl::class.java)
    }
}