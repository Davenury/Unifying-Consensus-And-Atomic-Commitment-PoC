package com.github.davenury.ucac.utils

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.createApplication
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.slf4j.LoggerFactory

class TestApplicationSet(
    peersetConfiguration: Map<String, List<String>>,
    signalListeners: Map<String, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<String, Map<String, Any>> = emptyMap(),
    val appsToExclude: List<String> = emptyList(),
) : AutoCloseable {
    private val apps: Map<PeerId, ApplicationUcac>
    private val peerAddresses: Map<PeerId, PeerAddress>
    private val peersets: Map<PeersetId, List<PeerId>>

    init {
        peersets = peersetConfiguration
            .mapKeys { PeersetId(it.key) }
            .mapValues { entry -> entry.value.map { PeerId(it) } }

        val peerIds: Set<PeerId> = peersetConfiguration.values.flatten().map { PeerId(it) }.toSet()
        val testConfigOverrides = mapOf(
            "peers" to peerIds.joinToString(";") { peerId -> "$peerId=${NON_RUNNING_PEER}" },
            "peersets" to peersetConfiguration.entries.joinToString(";") { (peersetId, peers) ->
                "$peersetId=${peers.joinToString(",")}"
            },
            "port" to 0,
            "host" to "localhost",
        )

        val peersToValidate = signalListeners.keys + configOverrides.keys + appsToExclude
        peersToValidate.map { PeerId(it) }
            .forEach { peerId -> Assert.assertTrue(peerId in peerIds) }

        apps = peerIds.associateWith { peerId ->
            createApplication(
                signalListeners[peerId.toString()] ?: emptyMap(),
                mapOf("peerId" to peerId) +
                        testConfigOverrides +
                        (configOverrides[peerId.toString()] ?: emptyMap()),
            )
        }

        // start and address discovery
        apps.filter { (peerId, _) -> !appsToExclude.contains(peerId.toString()) }
            .forEach { (_, app) -> app.startNonblocking() }

        peerAddresses = apps.mapValues { (peerId, app) ->
            val address = if (peerId.toString() in appsToExclude) {
                NON_RUNNING_PEER
            } else {
                "localhost:${app.getBoundPort()}"
            }
            PeerAddress(peerId, address)
        }

        apps.filter { (peerId, _) -> !appsToExclude.contains(peerId.toString()) }
            .forEach { (_, app) ->
                app.setPeerAddresses(peerAddresses)
            }

        logger.info("Apps ready")
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        logger.info("Stopping apps")
        runBlocking {
            apps.map { (_, app) -> GlobalScope.launch { app.stop(gracePeriodMillis, timeoutPeriodMillis) } }
                .forEach { it.join() }
        }
    }

    fun getPeersets(): Map<PeersetId, List<PeerId>> = peersets

    fun getPeer(peerId: String): PeerAddress = getPeer(PeerId(peerId))
    fun getPeer(peerId: PeerId): PeerAddress = peerAddresses[peerId]!!

    fun getPeerAddresses(): Map<PeerId, PeerAddress> = peerAddresses

    fun getPeerAddresses(peersetId: String): Map<PeerId, PeerAddress> = getPeerAddresses(PeersetId(peersetId))
    fun getPeerAddresses(peersetId: PeersetId): Map<PeerId, PeerAddress> =
        peersets[peersetId]!!.associateWith { peerId -> getPeer(peerId) }

    fun getRunningPeers(peersetId: String) =
        getPeerAddresses(peersetId)
            .filter {
                it.value.address != NON_RUNNING_PEER
            }

    fun getRunningApps(): List<ApplicationUcac> = apps
        .filter { (peerId, _) -> peerId.toString() !in appsToExclude }
        .values.toList()

    fun getApp(peerId: String): ApplicationUcac = getApp(PeerId(peerId))
    fun getApp(peerId: PeerId): ApplicationUcac = apps[peerId]!!

    companion object {
        const val NON_RUNNING_PEER: String = "localhost:0"
        private val logger = LoggerFactory.getLogger(TestApplicationSet::class.java)
    }

    override fun close() = stopApps()
}
