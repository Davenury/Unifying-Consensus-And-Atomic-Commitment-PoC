package com.github.davenury.ucac.commitment

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.Logger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
abstract class AbstractAtomicCommitmentProtocol(
    val logger: Logger,
    val peerResolver: PeerResolver,
) : AtomicCommitmentProtocol {

    val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    abstract suspend fun performProtocol(change: Change)

    abstract fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()

        changeIdToCompletableFuture[change.id] = cf

        with(CoroutineScope(executorService)) {
            launch(MDCContext()) {
                performProtocol(change)
            }
        }

        return cf
    }

    fun close() {
        executorService.close()
    }

    fun getPeersFromChange(change: Change): List<List<PeerAddress>> {
        if (change.peersets.isEmpty()) throw IllegalStateException("Change without peersetIds")
        return change.peersets.sortedBy { it.peersetId }.map { peersetInfo ->
            peerResolver.getPeersFromPeerset(peersetInfo.peersetId)
        }.map { peerset -> peerset.filter { it.globalPeerId != peerResolver.currentPeerAddress().globalPeerId } }
    }

    fun getPeerName() = peerResolver.currentPeer().toString()

    companion object {
        private val executorService: ExecutorCoroutineDispatcher =
            Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}
