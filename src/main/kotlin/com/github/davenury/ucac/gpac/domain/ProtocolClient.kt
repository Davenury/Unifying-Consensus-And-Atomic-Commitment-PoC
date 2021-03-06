package com.github.davenury.ucac.gpac.domain

import com.github.davenury.ucac.httpClient
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory

data class ResponsesWithErrorAggregation<K>(
    val responses: List<List<K>>,
    val aggregatedValue: Int
)

interface ProtocolClient {
    suspend fun sendElectMe(otherPeers: List<List<String>>, message: ElectMe): ResponsesWithErrorAggregation<ElectedYou>
    suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>>
    suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<Int>
}

class ProtocolClientImpl : ProtocolClient {

    override suspend fun sendElectMe(
        otherPeers: List<List<String>>,
        message: ElectMe
    ): ResponsesWithErrorAggregation<ElectedYou> =
        sendRequests<ElectMe, ElectedYou>(
            otherPeers,
            message,
            "elect",
            { singlePeer, e -> "Peer $singlePeer responded with exception: $e - election" },
            { accs -> accs.maxOf { it } }
        )

    override suspend fun sendFTAgree(otherPeers: List<List<String>>, message: Agree): List<List<Agreed>> =
        sendRequests<Agree, Agreed>(
            otherPeers,
            message,
            "ft-agree",
            { singlePeer, e -> "Peer $singlePeer responded with exception: $e - ft agreement" }
        ).responses

    override suspend fun sendApply(otherPeers: List<List<String>>, message: Apply): List<Int> =
        sendRequests<Apply, HttpResponse>(
            otherPeers, message, "apply",
            { it, e -> "Peer: $it didn't apply transaction: $e" }
        ).responses.flatten().map { it.status.value }

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: List<List<String>>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (String, Throwable) -> String,
        crossinline aggregateErrors: (accs: List<Int>) -> Int = { _: List<Int> -> 0 }
    ): ResponsesWithErrorAggregation<K> {
        val acc = mutableListOf<Int>()
        val responses: List<List<K>> = otherPeers.map { peersets ->
            peersets.map {
                CoroutineScope(Dispatchers.IO).async {
                    val (httpResult, value) = httpCall<K, T>("http://$it/$urlPath", requestBody, errorMessage)
                    acc.add(value)
                    httpResult
                }
            }
        }.map { jobs ->
            jobs.mapNotNull { job ->
                try {
                    job.await()
                } catch (e: Exception) {
                    logger.error("Error while evaluating responses: $e")
                    null
                }
            }
        }
            .also { logger.info("Got responses: $it") }

        return ResponsesWithErrorAggregation(
            responses,
            aggregateErrors(acc)
        )
    }

    private suspend inline fun <reified K, T> httpCall(
        url: String,
        requestBody: T,
        errorMessage: (String, Throwable) -> String
    ): Pair<K?, Int> =
        try {
            logger.info("Sending to: $url")
            httpClient.post<K>(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = requestBody!!
            }.let {
                Pair(it, 0)
            }
        } catch (e: ClientRequestException) {
            // since we're updating ballot number in electing phase, this mechanism lets us
            // get any aggregation from all responses from "Not electing you" response, so we can get
            // max of all ballotNumbers sent back to the leader
            logger.error(errorMessage(url, e))
            if (e.response.status.value == 422) {
                val value = e.response.content.readUTF8Line()?.let {
                    return@let Regex("[0-9]+").findAll(it)
                        .map(MatchResult::value)
                        .toList()
                        .map { it.toInt() }
                }?.get(0) ?: 0
                Pair(null, value)
            } else {
                Pair(null, 0)
            }
        } catch (e: Exception) {
            logger.error(errorMessage(url, e))
            Pair(null, 0)
        }

    companion object {
        private val logger = LoggerFactory.getLogger(ProtocolClientImpl::class.java)
    }
}