package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.GPACInstanceNotFoundException
import com.github.davenury.common.history.History
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class GPACFactory(
    private val transactionBlocker: TransactionBlocker,
    private val history: History,
    private val gpacConfig: GpacConfig,
    private val context: ExecutorCoroutineDispatcher,
    private val signalPublisher: SignalPublisher,
    private val peerResolver: PeerResolver
) {

    private val changeIdToGpacInstance: MutableMap<String, GPACProtocolAbstract> = mutableMapOf()
    private val mutex = Mutex()

    suspend fun getOrCreateGPAC(changeId: String): GPACProtocolAbstract =
        mutex.withLock {
            changeIdToGpacInstance[changeId] ?:
            GPACProtocolImpl(
                history,
                gpacConfig,
                context,
                GPACProtocolClientImpl(),
                transactionBlocker,
                signalPublisher,
                peerResolver
            ).also {
                changeIdToGpacInstance[changeId] = it
            }
        }

    suspend fun handleElect(message: ElectMe) =
        getOrCreateGPAC(message.change.id).handleElect(message)

    suspend fun handleAgree(message: Agree) =
        changeIdToGpacInstance[message.change.id]?.handleAgree(message) ?: throw GPACInstanceNotFoundException(message.change.id)

    suspend fun handleApply(message: Apply) =
        changeIdToGpacInstance[message.change.id]?.handleApply(message) ?: throw GPACInstanceNotFoundException(message.change.id)

    fun getChangeStatus(changeId: String) =
        changeIdToGpacInstance[changeId]?.getChangeResult(changeId)

}
