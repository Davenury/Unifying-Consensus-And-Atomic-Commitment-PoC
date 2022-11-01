package com.github.davenury.ucac.utils

import com.github.davenury.ucac.Application
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.createApplication
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersInPeersets: List<Int>,
    signalListeners: Map<Int, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<Int, Map<String, Any>> = emptyMap(),
    val appsToExclude: List<Int> = emptyList()
) {

    private var apps: MutableList<MutableList<Application>> = mutableListOf()
    private val peers: List<List<String>>

    init {
        val numberOfPeersets = numberOfPeersInPeersets.size
        val testConfigOverrides = mapOf(
            "ratis.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            }.joinToString(";") { it.joinToString(",") },
            "peers" to (1..numberOfPeersets).map { (1..numberOfPeersInPeersets[it - 1]).map { NON_RUNNING_PEER } }
                .joinToString(";") { it.joinToString(",") },
            "port" to 0,
            "host" to "localhost",
        )

        var currentApp = 0
        apps = (1..numberOfPeersets).map { peersetId ->
            (1..numberOfPeersInPeersets[peersetId - 1]).map { peerId ->
                currentApp++
                createApplication(
                    signalListeners[currentApp] ?: emptyMap(),
                    mapOf("peerId" to peerId, "peersetId" to peersetId) +
                            testConfigOverrides +
                            (configOverrides[peerId] ?: emptyMap()),
                )
            }.toMutableList()
        }.toMutableList()

        // start and address discovery
        apps
            .flatten()
            .filterIndexed { index, _ -> !appsToExclude.contains(index + 1) }
            .forEach { it.startNonblocking() }
        peers =
            apps.flatten()
                .asSequence()
                .mapIndexed { index, it ->
                    val address = if (index + 1 in appsToExclude) NON_RUNNING_PEER else "localhost:${it.getBoundPort()}"
                    Pair(it, address)
                }
                .groupBy{ it.first.getPeersetId() }
                .values
                .map { it.map { it.second } }
                .toList()

        apps
            .flatten()
            .zip(peers.flatten())
            .filterIndexed { index, _ -> !appsToExclude.contains(index + 1) }
            .forEach { (app, peer) ->
                peers
                    .map { it.filterNot { it == peer } }
                    .withIndex()
                    .associate { it.index + 1 to it.value }
                    .let {
                        app.setPeers(it, peer)
                    }
        }
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        apps.flatten().forEach { it.stop(gracePeriodMillis, timeoutPeriodMillis) }
    }

    fun getPeers() = peers

    fun getRunningPeers() = peers.map { it.filter { it != NON_RUNNING_PEER } }

    fun getApps() = apps
    fun getRunningApps(): List<Application> = apps
        .flatten()
        .zip(peers.flatten())
        .filterIndexed { index, _ -> !appsToExclude.contains(index + 1) }
        .map { it.first }

    companion object {
        const val NON_RUNNING_PEER: String = "localhost:0"
    }
}
