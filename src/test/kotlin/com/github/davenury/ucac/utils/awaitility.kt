package com.github.davenury.ucac.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.awaitility.Awaitility.await
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit

fun eventually(timeout: Long = 10L, fn: () -> Unit) {
    await().atMost(timeout, TimeUnit.SECONDS).untilAsserted(fn)
}

fun atLeast(timeout: Long = 1L, fn: () -> Unit) {
    await().atLeast(timeout, TimeUnit.SECONDS).and().atMost(10, TimeUnit.SECONDS).untilAsserted(fn)
}

suspend fun Phaser.arriveAndAwaitAdvanceWithTimeout(timeout: Duration = Duration.ofMinutes(2)) {
    withContext(Dispatchers.IO) {
        awaitAdvanceInterruptibly(arrive(), timeout.toMillis(), TimeUnit.MILLISECONDS)
    }
}
