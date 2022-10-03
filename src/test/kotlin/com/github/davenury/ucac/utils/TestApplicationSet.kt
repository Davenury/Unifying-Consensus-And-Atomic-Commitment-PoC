package com.github.davenury.ucac.utils

import com.github.davenury.ucac.*
import com.github.davenury.ucac.SignalListener
import java.util.*
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersets: Int,
    numberOfPeersInPeersets: List<Int>,
    signalListeners: Map<Int, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<Int, Map<String, Any>> = emptyMap(),
    val appsToExclude: List<Int> = emptyList()
) {

    private var apps: MutableList<MutableList<Application>> = mutableListOf()
    private val peers: List<List<String>>
    private val nonRunningPeer = "localhost:0"

    init {
        val ratisConfigOverrides = mapOf(
            "raft.server.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            },
            "raft.clusterGroupIds" to List(numberOfPeersets) { UUID.randomUUID() }
        )

        val addressesOverrides = mapOf(
            "peers.peersAddresses" to (1..numberOfPeersets).map { (1..numberOfPeersInPeersets[it - 1]).map { nonRunningPeer } }
        )

        var currentApp = 0
        apps = (1..numberOfPeersets).map { peersetId ->
            (1..numberOfPeersInPeersets[peersetId - 1]).map { peerId ->
                currentApp++
                createApplication(
                    arrayOf("$peerId", "$peersetId"),
                    signalListeners[currentApp] ?: emptyMap(),
                    ratisConfigOverrides + addressesOverrides + (configOverrides[peerId] ?: emptyMap()),
                    TestApplicationMode(peerId, peersetId)
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
                    val address = if (index + 1 in appsToExclude) nonRunningPeer else "localhost:${it.getBoundPort()}"
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

    fun getRunningPeers() = peers.map { it.filter { it != nonRunningPeer } }

    fun getApps() = apps
    fun getRunningApps(): List<Application> = apps
        .flatten()
        .zip(peers.flatten())
        .filterIndexed { index, _ -> !appsToExclude.contains(index + 1) }
        .map { it.first }

}
