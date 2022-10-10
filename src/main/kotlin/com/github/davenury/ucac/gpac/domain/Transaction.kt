package com.github.davenury.ucac.gpac.domain

import com.github.davenury.ucac.common.Change

data class Transaction(
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int = 0,
    val acceptVal: Accept? = null,
    val decision: Boolean = false,
    val ended: Boolean = false,
    val change: Change? = null,
)
