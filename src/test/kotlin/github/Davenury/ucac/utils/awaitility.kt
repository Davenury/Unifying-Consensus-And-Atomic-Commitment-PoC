package github.davenury.ucac.utils

import org.awaitility.Awaitility.await
import java.util.concurrent.TimeUnit

fun eventually(timeout: Long = 10L, fn: () -> Unit) {
    await().atMost(timeout, TimeUnit.SECONDS).untilAsserted(fn)
}

fun atLeast(timeout: Long = 1L, fn: () -> Unit) {
    await().atLeast(timeout, TimeUnit.SECONDS).and().atMost(10, TimeUnit.SECONDS).untilAsserted(fn)
}