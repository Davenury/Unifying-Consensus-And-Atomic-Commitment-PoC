package com.github.davenury.ucac

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
    val rest: RestConfig = RestConfig(),
) {
    fun peerAddresses(): List<List<String>> = parsePeers(peers)
    fun peerAddresses(peersetId: Int): List<String> = peerAddresses()[peersetId - 1]
}

data class RatisConfig(
    val addresses: String = "",
) {
    fun peerAddresses(): List<List<String>> = parsePeers(addresses)
}

data class GpacConfig(
    val maxLeaderElectionTries: Int = 5,
    val leaderFailTimeout: Duration = Duration.ofSeconds(60),
    val backoffBound: Duration = Duration.ofSeconds(120),
)

data class RaftConfig(
    val heartbeatTimeout: Duration = Duration.ofSeconds(2),
    val leaderTimeout: Duration = Duration.ofSeconds(1),
)

data class RestConfig(
    val defaultSyncTimeout: Duration = Duration.ofMinutes(1),
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
