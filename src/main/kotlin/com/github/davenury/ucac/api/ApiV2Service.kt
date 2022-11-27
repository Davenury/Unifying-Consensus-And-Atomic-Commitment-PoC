package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.commitment.gpac.GPACProtocolAbstract
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import java.net.URLDecoder
import java.nio.charset.Charset
import java.time.Duration
import java.util.concurrent.CompletableFuture

class ApiV2Service(
    private val gpacProtocol: GPACProtocolAbstract,
    private val consensusProtocol: ConsensusProtocol,
    private val twoPC: TwoPC,
    private val history: History,
    private var config: Config,
) {
    private val queue: Channel<ProcessorJob> = Channel(Channel.Factory.UNLIMITED)
    private val worker: Thread = Thread(Worker(queue, gpacProtocol, consensusProtocol, twoPC))

    init {
        worker.start()
    }

    fun getChanges(): Changes {
        return Changes.fromHistory(history)
    }

    fun getChangeById(id: String): Change? {
        return history.getEntryFromHistory(id)?.let { Change.fromHistoryEntry(it) }
    }

    fun getChangeStatus(changeId: String): CompletableFuture<ChangeResult> =
        consensusProtocol.getChangeResult(changeId)
            ?: gpacProtocol.getChangeResult(changeId)
            ?: twoPC.getChangeResult(changeId)
            ?: throw ChangeDoesntExist(changeId)

    suspend fun addChange(job: ProcessorJob): CompletableFuture<ChangeResult> =
        job.also {
            logger.info("Service send job $job to queue")
            queue.send(it)
        }.completableFuture

    private suspend fun notify(notificationUrl: String?, change: Change, changeResult: ChangeResult) {
        val decodedUrl = URLDecoder.decode(notificationUrl, Charset.defaultCharset())
        try {
            httpClient.post<HttpStatement>(decodedUrl) {
                contentType(ContentType.Application.Json)
                body = Notification(change, changeResult)
            }
        } catch (e: Exception) {
            logger.error("Unable to send notification about completed change", e)
        }
    }

    suspend fun addChangeSync(
        job: ProcessorJob,
        timeout: Duration?,
    ): ChangeResult? = try {
        withTimeout(timeout ?: config.rest.defaultSyncTimeout) {
            addChange(job).await()
        }
    } catch (e: TimeoutCancellationException) {
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger("ApiV2Service")
    }
}
