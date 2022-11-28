package com.github.davenury.tests

import com.github.davenury.common.Change
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.nio.charset.Charset

interface Sender {
    suspend fun executeChange(address: String, change: Change): ChangeState
}

class HttpSender(
    private val ownAddress: String
): Sender {
    override suspend fun executeChange(address: String, change: Change): ChangeState {
        return try {
            logger.info("Sending change to $address")
            val response = httpClient.post<HttpStatement>("http://$address/v2/change/async") {
                parameter("notification_url", URLEncoder.encode("$ownAddress/api/v1/notification", Charset.defaultCharset()))
                parameter("enforce_gpac", true)
                accept(ContentType.Application.Json)
                contentType(ContentType.Application.Json)
                body = change
            }
            logger.info("Received: ${response.execute().status.value}")
            ChangeState.ACCEPTED
        } catch (e: Exception) {
            logger.error("Couldn't execute change with address: $address")
            // TODO - bump metrics
            ChangeState.REJECTED
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestsHttpSender")
    }
}