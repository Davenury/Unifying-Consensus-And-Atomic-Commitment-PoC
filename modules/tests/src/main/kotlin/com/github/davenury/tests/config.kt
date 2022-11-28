package com.github.davenury.tests

import com.github.davenury.common.parsePeers
import com.github.davenury.tests.strategies.GetPeersStrategy
import com.github.davenury.tests.strategies.RandomPeersStrategy
import com.github.davenury.tests.strategies.RandomPeersWithDelayOnConflictStrategy
import com.sksamuel.hoplite.*
import com.sksamuel.hoplite.decoder.Decoder
import com.sksamuel.hoplite.fp.invalid
import com.sksamuel.hoplite.fp.valid
import java.time.Duration
import java.util.*
import kotlin.reflect.KType

data class Config(
    val peers: String,
    val notificationServiceAddress: String,
    val numberOfRequestsToSendToSinglePeerset: Int,
    val numberOfRequestsToSendToMultiplePeersets: Int,
    val durationOfTest: Duration,
    val maxPeersetsInChange: Int,
    val strategy: Strategy,
    val pushGatewayAddress: String,
) {
    fun peerAddresses(): Map<Int, List<String>> =
        parsePeers(peers)
            .withIndex()
            .associate { it.index to it.value }

    fun getStrategy(): GetPeersStrategy {
        val range = (0 until peerAddresses().size)
        return strategy.getStrategy(range)
    }
}

enum class Strategy {
    RANDOM {
        override fun getStrategy(range: IntRange): GetPeersStrategy =
            RandomPeersStrategy(range)
    },
    DELAY_ON_CONFLICTS {
        override fun getStrategy(range: IntRange): GetPeersStrategy =
            RandomPeersWithDelayOnConflictStrategy(range)
    };

    abstract fun getStrategy(range: IntRange): GetPeersStrategy
}

class StrategyDecoder: Decoder<Strategy> {
    override fun decode(node: Node, type: KType, context: DecoderContext): ConfigResult<Strategy>
        = when (node) {
            is StringNode ->
                try {
                    Strategy.valueOf(node.value.uppercase(Locale.getDefault())).valid()
                } catch (e: java.lang.IllegalArgumentException) {
                    ConfigFailure.Generic("Invalid enum value: ${node.value}. Expected one of ${Strategy.values().map { it.toString() }}").invalid()
                }
            else -> ConfigFailure.DecodeError(node, type).invalid()
        }

    override fun supports(type: KType): Boolean = type.classifier == Strategy::class
}