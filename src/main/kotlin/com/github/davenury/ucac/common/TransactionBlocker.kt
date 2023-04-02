package com.github.davenury.ucac.common

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.ProtocolName
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

class TransactionBlocker {
    private val semaphore = Semaphore(1)
    private val mutex = Mutex()
    private var protocol: ProtocolName? = null
    private var changeId: String? = null

    suspend fun isAcquired() = mutex.withLock {  semaphore.availablePermits < 1 }
    suspend fun isAcquiredByProtocol(protocol: ProtocolName) = isAcquired() && mutex.withLock {  this.protocol == protocol}

    suspend fun releaseBlock() = mutex.withLock {
        try {
            logger.info("Releasing transaction lock")
            semaphore.release()
            protocol = null
        } catch (e: Exception) {
            logger.error("Tried to release semaphore, when it was already released", e)
        }
    }

    suspend fun tryToBlock(protocol: ProtocolName, changeId: String) = mutex.withLock {
        val sameChange = changeId == this.changeId && protocol == this.protocol
        if (!semaphore.tryAcquire() && !sameChange) {
            throw AlreadyLockedException(this.protocol!!)
        }
        logger.info("Transaction lock acquired on changeId $changeId")
        this.protocol = protocol
        this.changeId = changeId
    }

    suspend fun getProtocolName(): ProtocolName? = mutex.withLock {  protocol}
    suspend fun getChangeId(): String? = mutex.withLock { changeId }

    //    TODO: Add changeId as parameter.
    suspend fun tryToReleaseBlockerChange(protocol: ProtocolName, changeId: String) {
        mutex.withLock {
            if (isAcquired() && (getProtocolName() != protocol || changeId != this.changeId))
                throw Exception("I tried to release TransactionBlocker from ${protocol.name} and change $changeId during being blocked by ${getProtocolName()?.name} and change ${this.changeId}")
        }
        releaseBlock()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("transaction-blocker")
    }
}
