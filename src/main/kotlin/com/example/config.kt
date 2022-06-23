package com.example


import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.MapPropertySource
import com.sksamuel.hoplite.addFileSource
import com.sksamuel.hoplite.addResourceSource
import java.io.File

data class Config(val raft: RaftConfig, val peers: PeersConfig, val protocol: ProtocolConfig)
data class PeersConfig(val peersAddresses: List<List<String>>, val maxLeaderElectionTries: Int)
data class RaftConfig(val server: ServerConfig, val clusterGroupIds: List<String>)
data class ServerConfig(val addresses: List<List<String>>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)
data class ProtocolConfig(val leaderFailTimeoutInSecs: Int)

fun loadConfig(overrides: Map<String, Any> = emptyMap()) =
    ConfigLoaderBuilder
        .default()
        .addSource(MapPropertySource(overrides))
        .addResourceSource("/${System.getenv("CONFIG_FILE") ?: "application.conf"}")
        .build()
        .loadConfigOrThrow<Config>()