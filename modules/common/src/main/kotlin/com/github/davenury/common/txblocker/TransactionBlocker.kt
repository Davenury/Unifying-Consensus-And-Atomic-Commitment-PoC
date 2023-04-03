package com.github.davenury.common.txblocker

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.NotLockedException
import com.github.davenury.common.ProtocolName

interface TransactionBlocker {
    fun getAcquisition(): TransactionAcquisition?

    @Deprecated("Promotes race conditions")
    fun isAcquired(): Boolean {
        return getAcquisition() != null
    }

    @Deprecated("Promotes race conditions")
    fun isAcquiredByProtocol(protocol: ProtocolName): Boolean {
        return getAcquisition()?.protocol == protocol
    }

    fun tryAcquireReentrant(acquisition: TransactionAcquisition): Boolean {
        return try {
            acquireReentrant(acquisition)
            true
        } catch (e: AlreadyLockedException) {
            false
        }
    }

    @Throws(AlreadyLockedException::class)
    fun acquireReentrant(acquisition: TransactionAcquisition)

    fun tryRelease(acquisition: TransactionAcquisition): Boolean {
        return try {
            release(acquisition)
            true
        } catch (e: NotLockedException) {
            false
        }
    }

    @Throws(NotLockedException::class)
    fun release(acquisition: TransactionAcquisition)

    @Deprecated("Promotes race conditions")
    fun getProtocolName(): ProtocolName? = getAcquisition()?.protocol

    @Deprecated("Promotes race conditions")
    fun getChangeId(): String? = getAcquisition()?.changeId
}
