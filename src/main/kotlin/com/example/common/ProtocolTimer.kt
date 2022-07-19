package com.example.common

<<<<<<< HEAD
import kotlinx.coroutines.*
import java.time.Duration
=======
>>>>>>> 76dbf88 (The first version of consensus implementation, where consensus tests sometimes pass)
import java.util.*
import kotlinx.coroutines.*

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
    fun setDelay(delay: Int)
}

<<<<<<< HEAD
class ProtocolTimerImpl(
    private val delay: Duration,
    private val backoffBound: Duration
) : ProtocolTimer {

    private var task: Job? = null
=======
class ProtocolTimerImpl(private var delay: Int, private val backoffBound: Long) : ProtocolTimer {

    private var task: Job? = null

>>>>>>> 76dbf88 (The first version of consensus implementation, where consensus tests sometimes pass)

    companion object {
        private val randomGenerator = Random()
    }

    override suspend fun startCounting(action: suspend () -> Unit) {
        cancelCounting()
<<<<<<< HEAD
        task = GlobalScope.launch(Dispatchers.IO) {
            delay(delay.toMillis() + randomGenerator.nextLong() % backoffBound.toMillis())
            action()
        }
=======
        task =
            GlobalScope.launch(Dispatchers.IO) {
                delay(delay + randomGenerator.nextLong() % backoffBound)
                action()
            }
>>>>>>> 76dbf88 (The first version of consensus implementation, where consensus tests sometimes pass)
    }

    override fun cancelCounting() {
        this.task?.cancel()
    }

    override fun setDelay(delay: Int) {
        this.delay = delay
    }
}
