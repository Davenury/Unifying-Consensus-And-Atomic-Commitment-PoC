package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.GPACInstanceNotFoundException
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class GPACFactory(
    private val peersetId: PeersetId,
    private val transactionBlocker: TransactionBlocker,
    private val history: History,
    private val config: Config,
    private val context: ExecutorCoroutineDispatcher,
    private val signalPublisher: SignalPublisher,
    private val peerResolver: PeerResolver
) {

    private val changeIdToGpacInstance: MutableMap<String, GPACProtocolAbstract> = mutableMapOf()
    private val mutex = Mutex()

    suspend fun getOrCreateGPAC(changeId: String): GPACProtocolAbstract =
        mutex.withLock {
            changeIdToGpacInstance[changeId] ?: GPACProtocolImpl(
                peersetId,
                history,
                config.gpac,
                context,
                GPACProtocolClientImpl(),
                transactionBlocker,
                peerResolver,
                signalPublisher,
                config.metricTest
            ).also {
                changeIdToGpacInstance[changeId] = it
            }
        }

    suspend fun handleElect(message: ElectMe) =
        getOrCreateGPAC(message.change.id).handleElect(message)

    suspend fun handleAgree(message: Agree) =
        getOrCreateGPAC(message.change.id).handleAgree(message)

    suspend fun handleApply(message: Apply) {
        getOrCreateGPAC(message.change.id)
            .handleApply(message)
    }

    fun getChangeStatus(changeId: String) =
        changeIdToGpacInstance[changeId]?.getChangeResult(changeId)

}
