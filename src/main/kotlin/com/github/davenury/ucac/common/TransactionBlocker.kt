package com.github.davenury.ucac.common

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.ProtocolName
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

class TransactionBlocker {
    private val semaphore = Semaphore(1)
    private var protocol: ProtocolName? = null
    private var changeId: String? = null

    fun isAcquired() = semaphore.availablePermits < 1
    fun isAcquiredByProtocol(protocol: ProtocolName) = isAcquired() && this.protocol == protocol

    fun releaseBlock() {
        try {
            logger.info("Releasing transaction lock")
            semaphore.release()
            protocol = null
        } catch (e: Exception) {
            logger.error("Tried to release semaphore, when it was already released", e)
        }
    }

    fun tryToBlock(protocol: ProtocolName, changeId: String) {
        val sameChange = changeId == this.changeId && protocol == this.protocol
        if (!semaphore.tryAcquire() && !sameChange) {
            throw AlreadyLockedException(this.protocol!!)
        }
        logger.info("Transaction lock acquired")
        this.protocol = protocol
        this.changeId = changeId
    }

    fun getProtocolName(): ProtocolName? = protocol
    fun getChangeId(): String? = changeId

    //    TODO: Add changeId as parameter.
    fun tryToReleaseBlockerChange(protocol: ProtocolName, changeId: String) {
        if (isAcquired() && (getProtocolName() != protocol || changeId != this.changeId))
            throw Exception("I tried to release TransactionBlocker from ${protocol.name} and change $changeId during being blocked by ${getProtocolName()?.name} and change ${this.changeId}")
        releaseBlock()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("transaction-blocker")
    }
}
