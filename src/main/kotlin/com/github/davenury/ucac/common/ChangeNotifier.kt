package com.github.davenury.ucac.common

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Notification
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import org.slf4j.LoggerFactory
import java.net.URLDecoder
import java.nio.charset.Charset

object ChangeNotifier {
    suspend fun notify(change: Change, changeResult: ChangeResult) {
        change.notificationUrl?.let {
            try {
                val response = httpClient.post<HttpStatement>(URLDecoder.decode(it, Charset.defaultCharset())) {
                    contentType(ContentType.Application.Json)
                    body = Notification(change, changeResult)
                }
                logger.info("Response from notifier: ${response.execute().status.value}")
            } catch (e: Exception) {
                logger.error("Error while sending notification to $it", e)
            }
        }
    }

    private val logger = LoggerFactory.getLogger("ChangeNotifier")
}