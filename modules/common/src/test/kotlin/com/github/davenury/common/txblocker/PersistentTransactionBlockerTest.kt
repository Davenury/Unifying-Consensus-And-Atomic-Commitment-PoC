package com.github.davenury.common.txblocker

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.NotLockedException
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.persistence.InMemoryPersistence
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNull
import strikt.assertions.isTrue

/**
 * @author Kamil Jarosz
 */
class PersistentTransactionBlockerTest {
    private lateinit var txblocker: PersistentTransactionBlocker

    @BeforeEach
    fun setUp() {
        txblocker = PersistentTransactionBlocker(InMemoryPersistence())
    }

    @Test
    fun noAcquisitionAtStart() {
        expectThat(txblocker.getAcquisition()).isNull()
    }

    @Test
    fun simpleAcquire() {
        val acquisition = TransactionAcquisition(ProtocolName.GPAC, "test")
        txblocker.acquireReentrant(acquisition)
        expectThat(txblocker.getAcquisition()).isEqualTo(acquisition)
        txblocker.release(acquisition)
        expectThat(txblocker.getAcquisition()).isNull()
    }

    @Test
    fun getAcquisitionTheSame() {
        val acquisition = TransactionAcquisition(ProtocolName.GPAC, "test")
        txblocker.acquireReentrant(acquisition)
        expectThat(txblocker.getAcquisition()).isEqualTo(acquisition)
        expectThat(txblocker.getAcquisition()).isEqualTo(acquisition)
    }

    @Test
    fun reentrantAcquire() {
        val acquisition = TransactionAcquisition(ProtocolName.GPAC, "test2")
        txblocker.acquireReentrant(acquisition)
        txblocker.acquireReentrant(acquisition)
        expectThat(txblocker.getAcquisition()).isEqualTo(acquisition)
    }

    @Test
    fun acquireAcquired() {
        val acquisition1 = TransactionAcquisition(ProtocolName.GPAC, "test1")
        val acquisition2 = TransactionAcquisition(ProtocolName.GPAC, "test2")
        txblocker.acquireReentrant(acquisition1)

        expectThat(txblocker.tryAcquireReentrant(acquisition1)).isTrue()
        expectThat(txblocker.tryAcquireReentrant(acquisition2)).isFalse()

        expectThrows<AlreadyLockedException> {
            txblocker.acquireReentrant(acquisition2)
        }
    }

    @Test
    fun releaseUnacquired() {
        val acquisition = TransactionAcquisition(ProtocolName.GPAC, "test")

        expectThat(txblocker.tryRelease(acquisition)).isFalse()

        expectThrows<NotLockedException> {
            txblocker.release(acquisition)
        }
    }

    @Test
    fun releaseWrongAcquired() {
        val acquisition1 = TransactionAcquisition(ProtocolName.GPAC, "test")
        val acquisition2 = TransactionAcquisition(ProtocolName.CONSENSUS, "test")

        txblocker.acquireReentrant(acquisition1)

        expectThat(txblocker.tryRelease(acquisition2)).isFalse()
        expectThat(txblocker.tryRelease(acquisition1)).isTrue()
        expectThat(txblocker.tryRelease(acquisition1)).isFalse()
    }
}
