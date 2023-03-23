package com.github.davenury.ucac

import com.github.davenury.common.*
import com.github.davenury.ucac.common.PeerResolver
import java.time.Duration

data class Config(
    val host: String,
    val port: Int,
    val peerId: String,
    // peer1=X;peer2=Y
    val peers: String,
    // peerset1=peer1,peer2;peerset2=peer3,peer4
    val peersets: String,

    val raft: RaftConfig = RaftConfig(),
    val gpac: GpacConfig = GpacConfig(),
    val twoPC: TwoPCConfig = TwoPCConfig(),
    val rest: RestConfig = RestConfig(),
    val persistence: PersistenceConfig = PersistenceConfig(),
    val metricTest: Boolean
) {
    fun peerId() = PeerId(peerId)

    // TODO remove:
    fun peersetId() = parsePeersets(peersets).filterValues { peers -> peers.contains(PeerId(peerId)) }.let {
        if (it.isEmpty()) {
            throw AssertionError("Peer in no peersets, not supported yet")
        }
        if (it.size != 1) {
            throw AssertionError("Peer in multiple peersets, not supported yet")
        }
        return@let it.keys.iterator().next()
    }

    fun newPeerResolver() = PeerResolver(peerId(), parsePeers(peers), parsePeersets(peersets))
}

data class TwoPCConfig(val changeDelay: Duration = Duration.ofSeconds(120))

data class GpacConfig(
    val maxLeaderElectionTries: Int = 5,
    val leaderFailDelay: Duration = Duration.ofSeconds(60),
    val leaderFailBackoff: Duration = Duration.ofSeconds(60),
    val retriesBackoffTimeout: Duration = Duration.ofSeconds(120),
    val initialRetriesDelay: Duration = Duration.ofSeconds(0),
    val ftAgreeRepeatDelay: Duration = Duration.ofMillis(500),
    val maxFTAgreeTries: Int = 5,
    val abortOnElectMe: Boolean = false,
    val phasesTimeouts: PhasesTimeouts = PhasesTimeouts(),
)

data class PhasesTimeouts(
    val electTimeout: Duration = Duration.ofSeconds(2),
    val agreeTimeout: Duration = Duration.ofSeconds(2),
    val applyTimeout: Duration = Duration.ofSeconds(2)
)

data class RaftConfig(
    val heartbeatTimeout: Duration = Duration.ofSeconds(2),
    val leaderTimeout: Duration = Duration.ofSeconds(1),
    val isEnabled: Boolean = true,
    val limitSize: Int = 200
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
