package com.github.davenury.common.txblocker

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.NotLockedException
import com.github.davenury.common.persistence.Persistence
import org.slf4j.LoggerFactory

class PersistentTransactionBlocker(private val persistence: Persistence) : TransactionBlocker {
    init {
        if (persistence.get(TX_BLOCKER_KEY) == null) {
            persistence.set(TX_BLOCKER_KEY, "")
        }
    }

    override fun getAcquisition(): TransactionAcquisition? {
        return persistence.get(TX_BLOCKER_KEY)
            ?.let { TransactionAcquisition.deserialize(it) }
    }

    private fun compareAndExchange(
        expected: TransactionAcquisition?,
        new: TransactionAcquisition?,
    ): TransactionAcquisition? {
        val witness = persistence.compareAndExchange(
            TX_BLOCKER_KEY,
            expected?.serialize() ?: "",
            new?.serialize() ?: "",
        )
        return witness?.let { TransactionAcquisition.deserialize(it) }
    }

    override fun acquireReentrant(acquisition: TransactionAcquisition) {
        val witness = compareAndExchange(null, acquisition)
        if (witness == acquisition) {
            logger.info("Re-entering an acquired transaction lock $acquisition")
            return
        }
        if (witness != null) {
            throw AlreadyLockedException(witness)
        }

        logger.info("Transaction lock acquired $acquisition")
    }

    override fun release(acquisition: TransactionAcquisition) {
        val witness = compareAndExchange(acquisition, null)
        if (witness != acquisition) {
            throw NotLockedException(acquisition)
        }

        logger.info("Transaction lock released $acquisition")
    }

    companion object {
        private val logger = LoggerFactory.getLogger("txblocker")
        private const val TX_BLOCKER_KEY = "txblocker"
    }
}
