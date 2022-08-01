package com.github.davenury.ucac.utils

import com.github.davenury.ucac.*
import com.github.davenury.ucac.EventListener
import java.util.*
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersets: Int,
    numberOfPeersInPeersets: List<Int>,
    actions: Map<Int, Map<TestAddon, suspend (ProtocolTestInformation) -> Unit>> = emptyMap(),
    eventListeners: Map<Int, List<EventListener>> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap()
) {

    private val apps: MutableList<MutableList<Application>> = mutableListOf()
    private val peers: List<List<String>>

    init {

        val ratisConfigOverrides = mapOf(
            "raft.server.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            },
            "raft.clusterGroupIds" to List(numberOfPeersets) { UUID.randomUUID() }
        )

        // applications creation
        repeat(numberOfPeersets) { peersetId ->
            val peersetApps = mutableListOf<Application>()
            repeat(numberOfPeersInPeersets[peersetId]) { peerId ->
                val absoluteIndex = apps.flatten().size + peerId + 1
                peersetApps.add(
                    createApplication(
                        arrayOf("${peerId + 1}", "${peersetId + 1}"),
                        actions[absoluteIndex] ?: emptyMap(),
                        eventListeners[absoluteIndex] ?: emptyList(),
                        ratisConfigOverrides + configOverrides,
                        TestApplicationMode(peerId + 1, peersetId + 1)
                    )
                )
            }
            apps.add(peersetApps)
        }

        // start and address discovery
        apps.flatten().forEach { app -> app.startNonblocking() }
        peers =
            apps.flatten()
                .asSequence()
                .withIndex()
                .groupBy{ it.value.getPeersetId() }
                .values
                .map { it.map { it.value } }
                .map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
                .toList()

        apps.flatten().forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        apps.flatten().forEach { it.stop(gracePeriodMillis, timeoutPeriodMillis) }
    }

    fun getPeers() = peers

}