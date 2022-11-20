package com.github.davenury.tests

import com.github.davenury.common.Change
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import java.net.URLEncoder
import java.nio.charset.Charset

interface Sender {
    suspend fun executeChange(address: String, change: Change)
}

class HttpSender(
    private val ownAddress: String
): Sender {
    override suspend fun executeChange(address: String, change: Change) {
        println("Sending $change to $address")
        val statement = httpClient.post<HttpStatement>("http://$address/v2/change/async") {
            parameter("notification_url", URLEncoder.encode("$ownAddress/api/v1/notification", Charset.defaultCharset()))
            parameter("enforce_gpac", true)
            accept(ContentType.Application.Json)
            contentType(ContentType.Application.Json)
            body = change
        }
        println("Status for change: $change ${statement.execute().status}")
    }
}