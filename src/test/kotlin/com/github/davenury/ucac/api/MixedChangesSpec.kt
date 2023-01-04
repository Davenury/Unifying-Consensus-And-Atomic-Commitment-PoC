package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectThat
import strikt.assertions.*
import java.io.File
import java.time.Duration
import java.util.concurrent.Phaser

@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class MixedChangesSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(MultiplePeersetSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `try to execute two change in the same time, first GPAC, then Raft`(): Unit = runBlocking {
        val applyEndPhaser = Phaser(6)
        val beforeSendingAgreePhaser = Phaser(1)
        val electionPhaser = Phaser(4)
        val applyConsensusPhaser = Phaser(2)

        listOf(applyEndPhaser, electionPhaser, beforeSendingAgreePhaser, applyConsensusPhaser)
            .forEach { it.register() }
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyEnd to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                applyEndPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected,
            Signal.BeforeSendingAgree to SignalListener {
                beforeSendingAgreePhaser.arrive()
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                applyConsensusPhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()
        val change = change(0, 1)
        val secondChange = change(mapOf(0 to change.toHistoryEntry(0).getId()))

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", change)

        beforeSendingAgreePhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", secondChange)

        applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

        applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))


//      First peerset
        askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(2)
            expectThat(changes[0]).isEqualTo(change)
            expectThat(changes[1]).isEqualTo(secondChange)
        }

        askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    private suspend fun executeChange(uri: String, change: Change): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { Pair(it, askForChanges(it)) }

    private fun change(vararg peersetIds: Int) = AddUserChange(
        peersetIds.map {
            ChangePeersetInfo(it, InitialHistoryEntry.getId())
        },
        "userName",
    )

    private fun change(peerSetIdToId: Map<Int, String>) = AddUserChange(
        peerSetIdToId.map { ChangePeersetInfo(it.key, it.value) },
        "userName",
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}