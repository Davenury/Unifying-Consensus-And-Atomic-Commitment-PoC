package com.github.davenury.tests

import com.github.davenury.common.*
import com.github.davenury.common.parsePeers
import com.github.davenury.tests.strategies.changes.CreateChangeStrategy
import com.github.davenury.tests.strategies.changes.DefaultChangeStrategy
import com.github.davenury.tests.strategies.changes.OnlyProcessableConflictsChangeStrategy
import com.github.davenury.tests.strategies.peersets.GetPeersStrategy
import com.github.davenury.tests.strategies.peersets.RandomPeersStrategy
import com.github.davenury.tests.strategies.peersets.RandomPeersWithDelayOnConflictStrategy
import com.sksamuel.hoplite.*
import com.sksamuel.hoplite.decoder.Decoder
import com.sksamuel.hoplite.fp.invalid
import com.sksamuel.hoplite.fp.valid
import java.lang.IllegalArgumentException
import java.time.Duration
import java.util.*
import kotlin.reflect.KType

data class Config(
    // peer1=X;peer2=Y
    val peers: String,
    // peerset1=peer1,peer2;peerset2=peer3,peer4
    val peersets: String,
    val notificationServiceAddress: String,
    val numberOfRequestsToSendToSinglePeerset: Int?,
    val numberOfRequestsToSendToMultiplePeersets: Int?,
    val maxPeersetsInChange: Int,
    val sendingStrategy: SendingStrategy,
    val createChangeStrategy: CreatingChangeStrategy,
    val pushGatewayAddress: String,
    val acProtocol: ACProtocolConfig,
    // TODO - after implementing multiple consensus this might come in handy
    val consensusProtocol: String? = null,
    val fixedPeersetsInChange: String? = null,
    val loadGeneratorConfig: LoadGeneratorConfig,
    val enforceConsensusLeader: Boolean = true,
) {
    fun peerAddresses(): Map<PeersetId, List<PeerAddress>> {
        val parsedPeers = parsePeers(peers)
        return parsePeersets(peersets)
            .mapValues { e -> e.value.map { parsedPeers[it]!! } }
    }

    fun getSendingStrategy(): GetPeersStrategy {
        return sendingStrategy.getStrategy(peerAddresses().keys.toList())
    }

    fun getCreateChangeStrategy(): CreateChangeStrategy {
        return createChangeStrategy.getStrategy(this.notificationServiceAddress)
    }
}

data class LoadGeneratorConfig(
    val loadGeneratorType: String,
    val constantLoad: String? = null,
    val timeOfSimulation: Duration? = null,
    val increasingLoadBound: Double? = null,
    val increasingLoadIncreaseDelay: Duration? = null,
    val increasingLoadIncreaseStep: Double? = null
)

enum class SendingStrategy {
    RANDOM {
        override fun getStrategy(peersets: List<PeersetId>): GetPeersStrategy =
            RandomPeersStrategy(peersets)
    },
    DELAY_ON_CONFLICTS {
        override fun getStrategy(peersets: List<PeersetId>): GetPeersStrategy =
            RandomPeersWithDelayOnConflictStrategy(peersets)
    };

    abstract fun getStrategy(peersets: List<PeersetId>): GetPeersStrategy
}

class StrategyDecoder : Decoder<SendingStrategy> {
    override fun decode(node: Node, type: KType, context: DecoderContext): ConfigResult<SendingStrategy> = when (node) {
        is StringNode ->
            try {
                SendingStrategy.valueOf(node.value.uppercase(Locale.getDefault())).valid()
            } catch (e: IllegalArgumentException) {
                ConfigFailure.Generic(
                    "Invalid enum value: ${node.value}. Expected one of ${
                        SendingStrategy.values().map { it.toString() }
                    }"
                ).invalid()
            }

        else -> ConfigFailure.DecodeError(node, type).invalid()
    }

    override fun supports(type: KType): Boolean = type.classifier == SendingStrategy::class
}

enum class CreatingChangeStrategy {
    DEFAULT {
        override fun getStrategy(ownAddress: String): CreateChangeStrategy =
            DefaultChangeStrategy(ownAddress)
    },
    PROCESSABLE_CONFLICTS {
        override fun getStrategy(ownAddress: String): CreateChangeStrategy =
            OnlyProcessableConflictsChangeStrategy(ownAddress)
    };

    abstract fun getStrategy(ownAddress: String): CreateChangeStrategy
}

class CreatingChangeStrategyDecoder : Decoder<CreatingChangeStrategy> {
    override fun decode(node: Node, type: KType, context: DecoderContext): ConfigResult<CreatingChangeStrategy> =
        when (node) {
            is StringNode ->
                try {
                    CreatingChangeStrategy.valueOf(node.value.uppercase(Locale.getDefault())).valid()
                } catch (e: IllegalArgumentException) {
                    ConfigFailure.Generic(
                        "Invalid enum value: ${node.value}. Expected one of ${
                            CreatingChangeStrategy.values().map { it.toString() }
                        }"
                    ).invalid()
                }

            else -> ConfigFailure.DecodeError(node, type).invalid()
        }

    override fun supports(type: KType): Boolean = type.classifier == CreatingChangeStrategy::class
}

enum class ACProtocol {
    TWO_PC {
        override fun getParam(enforceUsage: Boolean): String = "use_2pc=true"
    },
    GPAC {
        override fun getParam(enforceUsage: Boolean): String = "enforce_gpac=$enforceUsage"
    };

    abstract fun getParam(enforceUsage: Boolean): String
}

data class ACProtocolConfig(
    val enforceUsage: Boolean,
    val protocol: ACProtocol,
)

class ACProtocolDecoder : Decoder<ACProtocol> {
    override fun decode(node: Node, type: KType, context: DecoderContext): ConfigResult<ACProtocol> =
        when (node) {
            is StringNode ->
                try {
                    ACProtocol.valueOf(node.value.uppercase(Locale.getDefault())).valid()
                } catch (e: IllegalArgumentException) {
                    ConfigFailure.Generic(
                        "Invalid enum value: ${node.value}. Expected one of ${
                            ACProtocol.values().map { it.toString() }
                        }"
                    ).invalid()
                }

            else -> ConfigFailure.DecodeError(node, type).invalid()
        }

    override fun supports(type: KType): Boolean = type.classifier == ACProtocol::class
}
