package com.github.davenury.common

data class Notification(
    val change: Change,
    val result: ChangeResult,
    val sender: PeerAddress,
)