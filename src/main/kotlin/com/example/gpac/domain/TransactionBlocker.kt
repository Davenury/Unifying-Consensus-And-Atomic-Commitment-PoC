package com.example.gpac.domain

import com.example.common.AlreadyLockedException
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

interface TransactionBlocker {
    fun assertICanSendElectedYou()
    fun releaseBlock()
    fun tryToBlock()
}

class TransactionBlockerImpl: TransactionBlocker {
    private val semaphore = Semaphore(1)

    override fun assertICanSendElectedYou() =
        if (!semaphore.tryAcquire()) {
            logger.info("Tried to respond to elect me when semaphore acquired!")
            throw AlreadyLockedException()
        } else {
            semaphore.release()
        }

    override fun releaseBlock() {
        try {
            semaphore.release()
        } catch (e: Exception) {
            logger.error("Tried to release semaphore, when it was already released")
        }
    }

    override fun tryToBlock() {
        if (!semaphore.tryAcquire()) {
            throw AlreadyLockedException()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TransactionBlocker::class.java)
    }
}