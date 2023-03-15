package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.GPACInstanceNotFoundException
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.common.PeerResolver
import com.github.davenury.ucac.common.TransactionBlocker
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

class GPACFactory(
    private val transactionBlocker: TransactionBlocker,
    private val history: History,
    private val config: Config,
    private val context: ExecutorCoroutineDispatcher,
    private val signalPublisher: SignalPublisher,
    private val peerResolver: PeerResolver,
    private val gpacChannels: GPACChannels,
) {

    private val changeIdToGpacInstance: MutableMap<String, GPACProtocolAbstract> = mutableMapOf()
    private val mutex = Mutex()

    suspend fun getOrCreateGPAC(changeId: String): GPACProtocolAbstract =
        mutex.withLock {
            changeIdToGpacInstance[changeId] ?: GPACProtocolImpl(
                history,
                config.gpac,
                context,
                GPACProtocolClientImpl(peerResolver),
                transactionBlocker,
                signalPublisher,
                peerResolver,
                config.metricTest,
                GPACResponsesContainer(config.gpac.responsesTimeouts)
            ).also {
                changeIdToGpacInstance[changeId] = it
            }
        }

    fun startProcessing() {
        context.dispatch(Dispatchers.IO) {
            runBlocking {
                launch(MDCContext()) {
                    for (elect in gpacChannels.electMeChannel) {
                        try {
                            val result = handleElect(elect.message)
                            notifyLeader(elect.returnUrl, result)
                        } catch (e: Exception) {
                            logger.error("Error while handling elect message", e)
                        }
                    }
                }
                launch(MDCContext()) {
                    for (agree in gpacChannels.ftagreeChannel) {
                        try {
                            notifyLeader(agree.returnUrl, handleAgree(agree.message))
                        } catch (e: Exception) {
                            logger.error("Error while handling ftagree message", e)
                        }
                    }
                }
                launch(MDCContext()) {
                    for (apply in gpacChannels.applyChannel) {
                        try {
                            notifyLeader(apply.returnUrl, handleApply(apply.message))
                        } catch (e: Exception) {
                            logger.error("Error while handling apply message", e)
                        }
                    }
                }
                launch(MDCContext()) {
                    for (electResponse in gpacChannels.electResponseChannel) {
                        try {
                            handleElectResponse(electResponse)
                        } catch (e: Exception) {
                            logger.error("Error while handling electResponse", e)
                        }
                    }
                }
                launch(MDCContext()) {
                    for (agreeResponse in gpacChannels.agreedResponseChannel) {
                        try {
                            handleAgreeResponse(agreeResponse)
                        } catch (e: Exception) {
                            logger.error("Error while handling agreeResponse", e)
                        }
                    }
                }
                launch(MDCContext()) {
                    for (applyResponse in gpacChannels.appliedResponseChannel) {
                        try {
                            handleApplyResponse(applyResponse)
                        } catch (e: Exception) {
                            logger.error("Error while handling applyResponse", e)
                        }
                    }
                }
            }
        }
    }

    private suspend fun handleElectResponse(electResponse: ElectedYou) {
        changeIdToGpacInstance[electResponse.change.id]?.let {
            it.handleElectResponse(electResponse)
        }
    }

    private suspend fun handleAgreeResponse(agreeResponse: Agreed) {
        changeIdToGpacInstance[agreeResponse.change.id]?.let {
            it.handleAgreeResponse(agreeResponse)
        }
    }

    private suspend fun handleApplyResponse(applyResponse: Applied) {
        changeIdToGpacInstance[applyResponse.change.id]?.let {
            it.handleApplyResponse(applyResponse)
        }
    }

    private suspend fun handleElect(message: ElectMe) =
        getOrCreateGPAC(message.change.id).handleElect(message)

    private suspend fun handleAgree(message: Agree) =
        getOrCreateGPAC(message.change.id).handleAgree(message)

    private suspend fun handleApply(message: Apply) =
        getOrCreateGPAC(message.change.id)
            .handleApply(message)

    private suspend fun <T : Any> notifyLeader(returnUrl: String, result: T) {
        try {
            httpClient.post<HttpStatement>(returnUrl) {
                contentType(ContentType.Application.Json)
                body = result
            }.execute().apply {
                logger.info("Leader was notified about execution of message. Leader response: ${this.status.value}")
            }
        } catch (e: Exception) {
            logger.error("Error while notifying leader", e)
        }
    }

    fun getChangeStatus(changeId: String) =
        changeIdToGpacInstance[changeId]?.getChangeResult(changeId)

    companion object {
        private val logger = LoggerFactory.getLogger("GPACFactory")
    }
}
