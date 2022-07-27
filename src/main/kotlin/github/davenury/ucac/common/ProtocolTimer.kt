package github.davenury.ucac.common

import kotlinx.coroutines.*
import java.time.Duration
import java.util.*

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
}

class ProtocolTimerImpl(
    private val delay: Duration,
    private val backoffBound: Duration
) : ProtocolTimer {

    private var task: Job? = null

    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
        task = GlobalScope.launch(Dispatchers.IO) {
            delay(delay.toMillis() + randomGenerator.nextLong() % backoffBound.toMillis())
            action()
        }
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }
}