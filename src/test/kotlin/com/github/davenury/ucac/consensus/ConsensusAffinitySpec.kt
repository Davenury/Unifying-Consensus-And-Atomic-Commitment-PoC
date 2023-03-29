package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.httpClient
import com.github.davenury.ucac.routing.CurrentLeaderDto
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import java.util.concurrent.Phaser

class ConsensusAffinitySpec: IntegrationTestBase() {

    @Test
    fun `should pick up leader according to affinity`(): Unit = runBlocking {
        val phaser = Phaser(5)

        val peerLeaderElected = SignalListener {
            expectThat(phaser.phase).isEqualTo(0)
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf("peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4")),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                )
            },
            configOverrides = (0..4).map { "peer$it" }
                .associateWith { mapOf("raft.consensusAffinity" to "peerset0=peer3") }
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()

        val address = apps.getPeer("peer0").address

        val leaderId = httpClient.get<CurrentLeaderDto>("http://${address}/consensus/current-leader").currentLeaderPeerId?.peerId

        expectThat(leaderId).isEqualTo("peer3")
    }

    @Test
    fun `should not respect affinity if desired peer is not alive`(): Unit = runBlocking {
        val phaser = Phaser(4)

        val peerLeaderElected = SignalListener {
            expectThat(phaser.phase).isEqualTo(0)
            phaser.arrive()
        }

        apps = TestApplicationSet(
            mapOf("peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4")),
            appsToExclude = listOf("peer3"),
            signalListeners = (0..4).map { "peer$it" }.associateWith {
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                )
            },
            configOverrides = (0..4).map { "peer$it" }
                .associateWith { mapOf("raft.consensusAffinity" to "peerset0=peer3") }
        )

        phaser.arriveAndAwaitAdvanceWithTimeout()

        val address = apps.getPeer("peer0").address

        val leaderId = httpClient.get<CurrentLeaderDto>("http://${address}/consensus/current-leader").currentLeaderPeerId?.peerId

        expectThat(leaderId).isNotEqualTo("peer3")
    }

}