package com.github.davenury.ucac.gpac.domain

import com.github.davenury.ucac.common.Change

enum class Accept {
    COMMIT,
    ABORT;
}

data class ElectMe(val ballotNumber: Int, val change: Change, val acceptNum: Int? = null)
data class ElectedYou(
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int,
    val acceptVal: Accept?,
    val decision: Boolean
)

data class Agree(
    val ballotNumber: Int,
    val acceptVal: Accept,
    val change: Change,
    val decision: Boolean = false,
    val acceptNum: Int? = null
)

data class Agreed(val ballotNumber: Int, val acceptVal: Accept)
data class Apply(val ballotNumber: Int, val decision: Boolean, val acceptVal: Accept, val change: Change)
