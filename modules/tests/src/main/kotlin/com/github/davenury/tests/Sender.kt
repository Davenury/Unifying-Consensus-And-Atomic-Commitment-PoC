package com.github.davenury.tests

import com.github.davenury.common.*
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.Exception

interface Sender {
    suspend fun executeChange(address: PeerAddress, change: Change, peersetId: PeersetId): ChangeState
    suspend fun getConsensusLeaderId(address: PeerAddress, peersetId: PeersetId): PeerId?
}

class HttpSender(
    private val acProtocolConfig: ACProtocolConfig
): Sender {
    override suspend fun executeChange(address: PeerAddress, change: Change, peersetId: PeersetId): ChangeState {
        return try {
            logger.info("Sending $change to $address")
            Metrics.bumpSentChanges()
            val response = httpClient.post<HttpStatement>("http://${address.address}/v2/change/async?${acProtocolConfig.protocol.getParam(acProtocolConfig.enforceUsage)}&peerset=${peersetId.peersetId}") {
                accept(ContentType.Application.Json)
                contentType(ContentType.Application.Json)
                body = change
            }
            logger.info("Received: ${response.execute().status.value}")
            ChangeState.ACCEPTED
        } catch (e: Exception) {
            logger.error("Couldn't execute change with address: $address", e)
            when (e) {
                is ClientRequestException -> Metrics.reportUnsuccessfulChange(e.response.status.value)
                is ServerResponseException -> Metrics.reportUnsuccessfulChange(e.response.status.value)
                else -> throw e
            }
            ChangeState.REJECTED
        }
    }

    override suspend fun getConsensusLeaderId(address: PeerAddress, peersetId: PeersetId): PeerId? {
        return try {
            httpClient.get<PeersetInformation>("http://${address.address}/peerset-information?peerset=${peersetId.peersetId}").currentConsensusLeader
        } catch (e: IOException) {
            logger.error("Address: ${address.address} is dead, propagating exception", e)
            throw e
        } catch (e: Exception) {
            logger.error("Error while asking for consensus leader", e)
            null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestsHttpSender")
    }
}
