package com.github.davenury.ucac

import com.github.davenury.common.*
import com.github.davenury.ucac.common.PeerResolver
import java.time.Duration

data class Config(
    val port: Int,
    val peerId: String,
    // peer1=X;peer2=Y
    val peers: String,
    // peerset1=peer1,peer2;peerset2=peer3,peer4
    val peersets: String,

    val consensus: ConsensusConfig = ConsensusConfig(),
    val gpac: GpacConfig = GpacConfig(),
    val twoPC: TwoPCConfig = TwoPCConfig(),
    val rest: RestConfig = RestConfig(),
    val persistence: PersistenceConfig = PersistenceConfig(),
    val metricTest: Boolean,
    val experimentId: String?,
    val workerPoolSize: Int = 1,
    val configureTraces: Boolean = false,
) {
    fun peerId() = PeerId(peerId)

    fun peersetIds(): Set<PeersetId> = parsePeersets(peersets)
        .filterValues { peers -> peers.contains(PeerId(peerId)) }
        .keys

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

data class ConsensusConfig(
    val heartbeatTimeout: Duration = Duration.ofSeconds(2),
    val leaderTimeout: Duration = Duration.ofSeconds(1),
    val name: String = "raft",
    val isEnabled: Boolean = true,
    val maxChangesPerMessage: Int = 200
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
