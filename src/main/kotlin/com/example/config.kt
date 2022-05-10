package com.example

import com.example.domain.RaftPeerDto
import com.sksamuel.hoplite.ConfigLoader

data class Config(val raft: RaftConfig)
data class RaftConfig(val server: ServerConfig, val clusterGroupId: String)
data class ServerConfig(val peers: List<RaftPeerDto>, val root: RootConfig)
data class RootConfig(val storage: StorageConfig)
data class StorageConfig(val path: String)

fun loadConfig(configFile: String) =
    ConfigLoader().loadConfigOrThrow<Config>(configFile)