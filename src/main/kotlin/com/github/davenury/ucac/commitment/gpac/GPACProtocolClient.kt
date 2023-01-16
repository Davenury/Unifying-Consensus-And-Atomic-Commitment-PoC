package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.ucac.common.PeerAddress
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

data class ResponsesWithErrorAggregation<K>(
    val responses: List<List<K>>,
    val aggregatedValue: Int?
)

interface GPACProtocolClient {
    suspend fun sendElectMe(
        otherPeers: List<List<PeerAddress>>,
        message: ElectMe
    ): ResponsesWithErrorAggregation<ElectedYou>

    suspend fun sendFTAgree(otherPeers: List<List<PeerAddress>>, message: Agree): List<List<Agreed>>
    suspend fun sendApply(otherPeers: List<List<PeerAddress>>, message: Apply): List<Int>
}

class GPACProtocolClientImpl : GPACProtocolClient {

    override suspend fun sendElectMe(
        otherPeers: List<List<PeerAddress>>,
        message: ElectMe
    ): ResponsesWithErrorAggregation<ElectedYou> =
        sendRequests<ElectMe, ElectedYou>(
            otherPeers,
            message,
            "elect",
            { peer, e -> "Peer ${peer.globalPeerId} responded with exception: $e - election" },
            { accs: List<Int?> -> accs.filterNotNull().maxOfOrNull { it } }
        )

    override suspend fun sendFTAgree(otherPeers: List<List<PeerAddress>>, message: Agree): List<List<Agreed>> =
        sendRequests<Agree, Agreed>(
            otherPeers,
            message,
            "ft-agree",
            { peer, e -> "Peer ${peer.globalPeerId} responded with exception: $e - ft agreement" }
        ).responses

    override suspend fun sendApply(otherPeers: List<List<PeerAddress>>, message: Apply): List<Int> =
        sendRequests<Apply, HttpResponse>(
            otherPeers, message, "apply",
            { peer, e -> "Peer: ${peer.globalPeerId} didn't apply transaction: $e" }
        ).responses.flatten().map { it.status.value }

    private suspend inline fun <T, reified K> sendRequests(
        otherPeers: List<List<PeerAddress>>,
        requestBody: T,
        urlPath: String,
        crossinline errorMessage: (PeerAddress, Throwable) -> String,
        crossinline aggregateErrors: (accs: List<Int>) -> Int? = { _: List<Int> -> 0 }
    ): ResponsesWithErrorAggregation<K> {
        val acc = mutableListOf<Int?>()
        val responses: List<List<K>> = otherPeers.map { peerset ->
            peerset.map { peer ->
                CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                    val (httpResult, value) = gpacHttpCall<K, T>(
                        "http://${peer.address}/$urlPath",
                        requestBody,
                    ) { throwable -> errorMessage(peer, throwable) }
                    acc.add(value)
                    httpResult
                }
            }
        }.map { jobs ->
            jobs.mapNotNull { job ->
                try {
                    job.await()
                } catch (e: Exception) {
                    logger.error("Error while evaluating responses", e)
                    null
                }
            }
        }.also { logger.info("Got responses: $it") }

        return ResponsesWithErrorAggregation(
            responses,
            aggregateErrors(acc.filterNotNull())
        )
    }

    private suspend inline fun <reified Response, Message> gpacHttpCall(
        url: String,
        requestBody: Message,
        errorMessage: (Throwable) -> String
    ): Pair<Response?, Int> =
        try {
            logger.debug("Sending $requestBody to: $url")
            val response = httpClient.post<Response>(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = requestBody!!
            }
            Pair(response, 0)
        } catch (e: ClientRequestException) {
            // since we're updating ballot number in electing phase, this mechanism lets us
            // get any aggregation from all responses from "Not electing you" response, so we can get
            // max of all ballotNumbers sent back to the leader
            logger.error(errorMessage(e), e)
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
            logger.error(errorMessage(e), e)
            Pair(null, 0)
        }

    companion object {
        private val logger = LoggerFactory.getLogger("gpac-client")
    }
}
