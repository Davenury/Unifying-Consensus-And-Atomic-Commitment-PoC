package com.github.davenury.ucac.commitment

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import kotlinx.coroutines.*
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.Logger
import java.lang.IllegalStateException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
abstract class AbstractAtomicCommitmentProtocol(
    val logger: Logger,
    val peerResolver: PeerResolver,
): AtomicCommitmentProtocol {

    val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()
    private val executorService: ExecutorCoroutineDispatcher =
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    abstract suspend fun performProtocol(change: Change)

    abstract fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()

        val myAddress = peerResolver.currentPeerAddress().address
        val enrichedChange =
            if (change.peers.contains(myAddress)) {
                change
            } else {
                change.withAddress(myAddress)
            }

        changeIdToCompletableFuture[enrichedChange.toHistoryEntry().getId()] = cf

        with(CoroutineScope(executorService)) {
            launch(MDCContext()) {
                performProtocol(enrichedChange)
            }
        }

        return cf
    }

    fun close() {
        executorService.close()
    }

    fun getPeersFromChange(change: Change): List<List<PeerAddress>> {
        if (change.peers.isEmpty()) throw IllegalStateException("Change without peers")
        return change.peers.map { peer ->
            val peersetId = peerResolver.findPeersetWithPeer(peer)
            if (peersetId == null) {
                logger.error("Peer $peer not found in ${peerResolver.getPeers()}")
            }

            peerResolver
                .findPeersetWithPeer(peer)
                ?.let { peerResolver.getPeersFromPeerset(peersetId!!) } // TODO we need to specify peersetIds instead of peers
                ?: run {
                    logger.error("Peer $peer not found in ${peerResolver.getPeers()}")
                    throw IllegalStateException("That peer doesn't exist")
                }
        }.map { peerset -> peerset.filter { it.globalPeerId != peerResolver.currentPeerAddress().globalPeerId } }
    }

    fun getPeerName() = peerResolver.currentPeerAddress().globalPeerId.toString()


}
