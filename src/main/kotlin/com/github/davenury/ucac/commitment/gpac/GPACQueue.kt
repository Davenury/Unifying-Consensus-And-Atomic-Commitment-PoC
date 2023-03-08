package com.github.davenury.ucac.commitment.gpac

import kotlinx.coroutines.channels.Channel

class GPACQueue(
    private val channels: GPACChannels
) {

    suspend fun handleElect(electMe: ElectMe, returnUrl: String) {
        channels.electMeChannel.send(EnrichedElectMe(electMe, returnUrl))
    }

    suspend fun handleFtAgree(agree: Agree, returnUrl: String) {
        channels.ftagreeChannel.send(EnrichedAgree(agree, returnUrl))
    }

    suspend fun handleApply(apply: Apply, returnUrl: String) {
        channels.applyChannel.send(EnrichedApply(apply, returnUrl))
    }

    suspend fun handleElectedYou(electedYou: ElectedYou) {
        channels.electResponseChannel.send(electedYou)
    }

    suspend fun handleAgreed(agreed: Agreed) {
        channels.agreedResponseChannel.send(agreed)
    }

    suspend fun handleApplied(applied: Applied) {
        channels.appliedResponseChannel.send(applied)
    }

}

data class GPACChannels(
    val electMeChannel: Channel<EnrichedElectMe>,
    val ftagreeChannel: Channel<EnrichedAgree>,
    val applyChannel: Channel<EnrichedApply>,
    val electResponseChannel: Channel<ElectedYou>,
    val agreedResponseChannel: Channel<Agreed>,
    val appliedResponseChannel: Channel<Applied>,
) {
    companion object {
        fun create(): GPACChannels = GPACChannels(
            Channel(),
            Channel(),
            Channel(),
            Channel(),
            Channel(),
            Channel(),
        )
    }
}

data class EnrichedElectMe(
    val message: ElectMe,
    val returnUrl: String,
)
data class EnrichedAgree(
    val message: Agree,
    val returnUrl: String,
)
data class EnrichedApply(
    val message: Apply,
    val returnUrl: String,
)