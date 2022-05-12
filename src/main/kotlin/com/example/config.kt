package com.example

import com.sksamuel.hoplite.ConfigLoader

data class Config(val raft: RaftConfig, val peers: PeersConfig)
data class PeersConfig(val peersAddresses: List<String>, val maxLeaderElectionTries: Int)
data class RaftConfig(val server: ServerConfig, val clusterGroupId: String)
data class ServerConfig(val addresses: List<String>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)

fun loadConfig() =
    ConfigLoader().loadConfigOrThrow<Config>("/${System.getenv("CONFIG_FILE") ?: "application.conf"}")