package com.github.davenury.ucac.common.structure

import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*
import org.slf4j.LoggerFactory

class HttpSubscriber(
    val address: String
): Subscriber {
    override val type: String
        get() = "http"

    override suspend fun notifyConsensusLeaderChange(newLeaderPeerId: PeerId, newLeaderPeersetId: PeersetId) {
        try {
            httpClient.post<HttpResponseData>(address) {
                contentType(ContentType.Application.Json)
                body = CurrentLeaderFullInfoDto(newLeaderPeerId, newLeaderPeersetId)
            }
        } catch (e: Exception) {
            logger.error("Could not send notification to subscriber")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("HttpSubscriber")
    }
}