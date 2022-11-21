package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.AlreadyLockedException
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

class TransactionBlocker {
    private val semaphore = Semaphore(1)

    fun isAcquired() = semaphore.availablePermits < 1

    fun releaseBlock() {
        try {
            logger.info("Releasing transaction lock")
            semaphore.release()
        } catch (e: Exception) {
            logger.error("Tried to release semaphore, when it was already released")
        }
    }

    fun tryToBlock() {
        if (!semaphore.tryAcquire()) {
            throw AlreadyLockedException()
        }
        logger.info("Transaction lock acquired")
    }

    companion object {
        private val logger = LoggerFactory.getLogger("transaction-blocker")
    }
}
