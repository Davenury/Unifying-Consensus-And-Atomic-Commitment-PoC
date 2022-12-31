package com.github.davenury.ucac.utils

import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.createApplication
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.HashMap
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersInPeersets: List<Int>,
    signalListeners: Map<Int, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<Int, Map<String, Any>> = emptyMap(),
    val appsToExclude: List<Int> = emptyList()
) : AutoCloseable {

    private var apps: MutableList<MutableList<ApplicationUcac>> = mutableListOf()
    private val peers: MutableMap<GlobalPeerId, PeerAddress> = HashMap()

    init {
        val numberOfPeersets = numberOfPeersInPeersets.size
        val testConfigOverrides = mapOf(
            "ratis.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            }.joinToString(";") { it.joinToString(",") },
            "peers" to (0 until numberOfPeersets).map { (0 until numberOfPeersInPeersets[it]).map { NON_RUNNING_PEER } }
                .joinToString(";") { it.joinToString(",") },
            "port" to 0,
            "host" to "localhost",
        )

        var currentApp = 0
        apps = (0 until numberOfPeersets).map { peersetId ->
            (0 until numberOfPeersInPeersets[peersetId]).map { peerId ->
                createApplication(
                    signalListeners[currentApp] ?: emptyMap(),
                    mapOf("peerId" to peerId, "peersetId" to peersetId) +
                            testConfigOverrides +
                            (configOverrides[currentApp++] ?: emptyMap()),
                )
            }.toMutableList()
        }.toMutableList()

        validateAppIds(signalListeners.keys, currentApp)
        validateAppIds(configOverrides.keys, currentApp)
        validateAppIds(appsToExclude, currentApp)

        // start and address discovery
        apps
            .flatten()
            .filterIndexed { index, _ -> !appsToExclude.contains(index) }
            .forEach { it.startNonblocking() }

        apps.flatten().forEachIndexed { appId, app ->
            val address = if (appId in appsToExclude) {
                NON_RUNNING_PEER
            } else {
                "localhost:${app.getBoundPort()}"
            }
            val globalPeerId = app.getGlobalPeerId()
            peers[globalPeerId] = PeerAddress(globalPeerId, address)
        }

        apps.flatten()
            .filterIndexed { index, _ -> index !in appsToExclude }
            .forEach { app ->
                app.setPeers(peers)
            }

        logger.info("Apps ready")
    }

    private fun validateAppIds(
        appIds: Collection<Int>,
        appCount: Int,
    ) {
        if (appIds.isNotEmpty()) {
            val sorted = TreeSet(appIds)
            if (sorted.first() < 0 || sorted.last() >= appCount) {
                throw AssertionError("Wrong app IDs: $sorted (total number of apps: $appCount)")
            }
        }
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        logger.info("Stopping apps")
        apps.flatten().forEach { it.stop(gracePeriodMillis, timeoutPeriodMillis) }
    }

    fun getPeers(): Map<GlobalPeerId, PeerAddress> = Collections.unmodifiableMap(peers)

    fun getPeers(peersetId: Int) = peers.filter { it.key.peersetId == peersetId }

    fun getPeer(peersetId: Int, peerId: Int): PeerAddress = peers[GlobalPeerId(peersetId, peerId)]!!

    fun getRunningPeers(peersetId: Int) = peers.filter {
        it.value.address != NON_RUNNING_PEER && it.key.peersetId == peersetId
    }

    fun getRunningApps(): List<ApplicationUcac> = apps
        .flatten()
        .filterIndexed { index, _ -> index !in appsToExclude }

    fun getApp(globalPeerId: GlobalPeerId): ApplicationUcac =
        apps[globalPeerId.peersetId][globalPeerId.peerId]

    companion object {
        const val NON_RUNNING_PEER: String = "localhost:0"
        private val logger = LoggerFactory.getLogger(TestApplicationSet::class.java)
    }

    override fun close() = stopApps()
}
