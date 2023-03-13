package com.github.davenury.ucac.commitment.gpac

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.davenury.common.Change
import com.github.davenury.common.GlobalPeerId


enum class Accept {
    COMMIT,
    ABORT;
}

sealed class GpacResponse(open val sender: GlobalPeerId) {
    abstract fun isSuccess(): Boolean
}

data class ElectMe(val ballotNumber: Int, val change: Change, val acceptNum: Int? = null)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ElectedYou(
    val change: Change,
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int,
    val acceptVal: Accept?,
    val decision: Boolean,
    val elected: Boolean,
    val reason: Reason? = null,
    override val sender: GlobalPeerId
) : GpacResponse(sender) {
    override fun isSuccess(): Boolean = elected
}

data class Agree(
    val ballotNumber: Int,
    val acceptVal: Accept,
    val change: Change,
    val decision: Boolean = false,
    val acceptNum: Int? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Agreed(
    val change: Change,
    val ballotNumber: Int,
    val acceptVal: Accept,
    val agreed: Boolean,
    val reason: Reason? = null,
    override val sender: GlobalPeerId,
): GpacResponse(sender) {
    override fun isSuccess(): Boolean = agreed
}

data class Apply(val ballotNumber: Int, val decision: Boolean, val acceptVal: Accept, val change: Change)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Applied(val change: Change, override val sender: GlobalPeerId): GpacResponse(sender) {
    override fun isSuccess(): Boolean = true
}

enum class Reason {
    ALREADY_LOCKED, WRONG_BALLOT_NUMBER, NOT_VALID_LEADER, UNKNOWN
}
