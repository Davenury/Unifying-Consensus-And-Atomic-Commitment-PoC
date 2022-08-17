package com.github.davenury.ucac


import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.MapPropertySource
import com.sksamuel.hoplite.addResourceSource
import java.time.Duration

data class Config(val raft: RaftConfig, val peers: PeersConfig, val protocol: ProtocolConfig)
data class PeersConfig(val peersAddresses: List<List<String>>, val maxLeaderElectionTries: Int)
data class RaftConfig(val server: ServerConfig, val clusterGroupIds: List<String>)
data class ServerConfig(val addresses: List<List<String>>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)
data class ProtocolConfig(val leaderFailTimeout: Duration, val backoffBound: Duration)

fun loadConfig(overrides: Map<String, Any> = emptyMap()): Config {
    val configFile = System.getProperty("configFile")
        ?: System.getenv("CONFIG_FILE")
        ?: "application.conf"
    return ConfigLoaderBuilder
        .default()
        .addSource(MapPropertySource(overrides))
        .addResourceSource("/$configFile")
        .build()
        .loadConfigOrThrow()
}