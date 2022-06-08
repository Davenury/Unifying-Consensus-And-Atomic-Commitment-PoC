package com.example

import com.sksamuel.hoplite.ConfigLoader

data class Config(val raft: RaftConfig, val peers: PeersConfig, val protocol: ProtocolConfig)
data class PeersConfig(val peersAddresses: List<List<String>>, val maxLeaderElectionTries: Int)
data class RaftConfig(val server: ServerConfig, val clusterGroupIds: List<String>)
data class ServerConfig(val addresses: List<List<String>>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)
data class ProtocolConfig(val leaderFailTimeoutInSecs: Int)

fun loadConfig() =
    ConfigLoader().loadConfigOrThrow<Config>("/${System.getenv("CONFIG_FILE") ?: "application.conf"}")