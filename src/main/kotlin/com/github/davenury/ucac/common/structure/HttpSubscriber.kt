package com.github.davenury.ucac.common.structure

import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import org.slf4j.LoggerFactory

class HttpSubscriber(
    val address: String
): Subscriber {

    override suspend fun notifyConsensusLeaderChange(newLeaderPeerId: PeerId, newLeaderPeersetId: PeersetId) {
        logger.info("Sending new consensus leader message to $address")
        try {
            val response = httpClient.post<HttpStatement>(address) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
                body = CurrentLeaderFullInfoDto(newLeaderPeerId, newLeaderPeersetId)
            }
            logger.info("Sent new consensus leader change to notification service: ${response.execute().status.value}")
        } catch (e: Exception) {
            logger.error("Could not send notification to subscriber", e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("HttpSubscriber")
    }
}