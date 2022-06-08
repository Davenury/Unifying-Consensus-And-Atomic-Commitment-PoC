package com.example.domain

import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

interface TransactionBlocker {
    fun canISendElectedYou(): Boolean
    fun releaseBlock()
    fun tryToBlock()
}

class TransactionBlockerImpl: TransactionBlocker {
    private val semaphore = Semaphore(1)

    override fun canISendElectedYou() =
        if (!semaphore.tryAcquire()) {
            logger.info("Tried to respond to elect me when semaphore acquired!")
            false
        } else {
            semaphore.release()
            true
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