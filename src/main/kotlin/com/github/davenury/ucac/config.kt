package com.github.davenury.ucac

import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerResolver
import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.MapPropertySource
import com.sksamuel.hoplite.addResourceSource
import java.time.Duration

fun parsePeers(peers: String): List<List<String>> {
    return peers.split(";")
        .map { peerset -> peerset.split(",").map { it.trim() } }
}

data class Config(
    val host: String,
    val port: Int,
    val peerId: Int,
    val peersetId: Int,
    // peer1,peer2;peer3,peer4
    val peers: String,

    val raft: RaftConfig = RaftConfig(),
    val ratis: RatisConfig = RatisConfig(),
    val gpac: GpacConfig = GpacConfig(),
    val twoPC: TwoPCConfig = TwoPCConfig(),
    val rest: RestConfig = RestConfig(),
) {
    fun globalPeerId() = GlobalPeerId(peersetId, peerId)
    fun newPeerResolver() = PeerResolver(globalPeerId(), ArrayList(parsePeers(peers)))
}

data class RatisConfig(
    val addresses: String = "",
) {
    fun peerAddresses(): List<List<String>> = parsePeers(addresses)
}

data class TwoPCConfig(val changeDelay: Duration = Duration.ofSeconds(120))

data class GpacConfig(
    val maxLeaderElectionTries: Int = 5,
    val leaderFailDelay: Duration = Duration.ofSeconds(60),
    val retriesBackoffTimeout: Duration = Duration.ofSeconds(120),
    val initialRetriesDelay: Duration = Duration.ofSeconds(0)
)

data class RaftConfig(
    val heartbeatTimeout: Duration = Duration.ofSeconds(2),
    val leaderTimeout: Duration = Duration.ofSeconds(1),
)

data class RestConfig(
    val defaultSyncTimeout: Duration = Duration.ofMinutes(1)
)

fun loadConfig(overrides: Map<String, Any> = emptyMap()): Config {
    val configFile = System.getProperty("configFile")
        ?: System.getenv("CONFIG_FILE")
        ?: "application.conf"
    return loadConfig(configFile, overrides)
}

fun loadConfig(configFileName: String, overrides: Map<String, Any> = emptyMap()): Config =
    ConfigLoaderBuilder
        .default()
        .addSource(MapPropertySource(overrides))
        .addResourceSource("/$configFileName")
        .build()
        .loadConfigOrThrow()
