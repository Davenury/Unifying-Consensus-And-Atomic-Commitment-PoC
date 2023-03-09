package com.github.davenury.ucac

import com.github.davenury.ucac.common.GlobalPeerId
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import java.time.Duration

fun parsePeers(peers: String): Map<GlobalPeerId, PeerAddress> {
    val parsed: MutableMap<GlobalPeerId, PeerAddress> = HashMap()
    peers.split(";")
        .map { peerset: String -> peerset.split(",").map { it.trim() } }
        .forEachIndexed { peersetId: Int, peerset: List<String> ->
            peerset.forEachIndexed { peerId, address ->
                val globalPeerId = GlobalPeerId(peersetId, peerId)
                parsed[globalPeerId] = PeerAddress(globalPeerId, address)
            }
        }
    return parsed
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
    val persistence: PersistenceConfig = PersistenceConfig(),
    val metricTest: Boolean
) {
    fun globalPeerId() = GlobalPeerId(peersetId, peerId)
    fun newPeerResolver() = PeerResolver(globalPeerId(), parsePeers(peers))
}

data class RatisConfig(
    val addresses: String = "",
) {
    fun peerAddresses(): Map<GlobalPeerId, PeerAddress> = parsePeers(addresses)
}

data class TwoPCConfig(val changeDelay: Duration = Duration.ofSeconds(120))

data class GpacConfig(
    val maxLeaderElectionTries: Int = 5,
    val leaderFailDelay: Duration = Duration.ofSeconds(60),
    val retriesBackoffTimeout: Duration = Duration.ofSeconds(120),
    val initialRetriesDelay: Duration = Duration.ofSeconds(0),
    val responsesTimeouts: ResponsesTimeoutsConfig = ResponsesTimeoutsConfig.default(),
    val ftAgreeRepeatDelay: Duration = Duration.ofMillis(500)
)
data class ResponsesTimeoutsConfig(
    val electTimeout: Duration,
    val agreeTimeout: Duration,
    val applyTimeout: Duration
) {
    companion object {
        fun default() = ResponsesTimeoutsConfig(Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofMillis(200))
    }
}

data class RaftConfig(
    val heartbeatTimeout: Duration = Duration.ofSeconds(2),
    val leaderTimeout: Duration = Duration.ofSeconds(1),
    val isEnabled: Boolean = true
)

data class RestConfig(
    val defaultSyncTimeout: Duration = Duration.ofMinutes(1)
)

enum class PersistenceType {
    IN_MEMORY,
    REDIS,
}

data class PersistenceConfig(
    val type: PersistenceType = PersistenceType.IN_MEMORY,
    val redisHost: String? = null,
    val redisPort: Int? = null,
)
