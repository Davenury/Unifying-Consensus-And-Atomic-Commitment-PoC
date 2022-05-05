package com.example

import com.sksamuel.hoplite.ConfigLoader

data class Config(val raft: RaftConfig)
data class RaftConfig(val server: ServerConfig, val clusterGroupId: String)
data class ServerConfig(val addresses: List<String>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)

fun loadConfig() =
    ConfigLoader().loadConfigOrThrow<Config>("/application.conf")
