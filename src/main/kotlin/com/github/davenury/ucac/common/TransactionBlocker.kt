package com.github.davenury.ucac.common

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.ProtocolName
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

class TransactionBlocker {
    private val semaphore = Semaphore(1)
    private var protocol: ProtocolName? = null

    fun isAcquired() = semaphore.availablePermits < 1

    fun releaseBlock() {
        try {
            logger.info("Releasing transaction lock")
            semaphore.release()
            protocol = null
        } catch (e: Exception) {
            logger.error("Tried to release semaphore, when it was already released")
        }
    }

    fun tryToBlock(protocol: ProtocolName) {
        if (!semaphore.tryAcquire()) {
            throw AlreadyLockedException(this.protocol!!)
        }
        logger.info("Transaction lock acquired")
        this.protocol = protocol
    }

    fun getProtocolName(): ProtocolName? = protocol

    fun tryToReleaseBlockerAsProtocol(protocol: ProtocolName){
        if (isAcquired() && getProtocolName() != protocol)
            throw Exception("I tried to release TransactionBlocker from ${protocol.name} during being blocked by ${getProtocolName()?.name}")
        releaseBlock()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("transaction-blocker")
    }
}
