package com.github.davenury.ucac.commitment

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.commitment.gpac.GPACProtocolImpl
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.PeerResolver
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.Logger
import java.util.concurrent.CompletableFuture

/**
 * @author Kamil Jarosz
 */
interface AtomicCommitmentProtocol {

    suspend fun performProtocol(change: Change)

    fun getLogger(): Logger

    fun getPeerResolver(): PeerResolver

    fun putChangeToCompletableFutureMap(change: Change, completableFuture: CompletableFuture<ChangeResult>)

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()

        val myAddress = getPeerResolver().currentPeerAddress().address
        val enrichedChange =
            if (change.peers.contains(myAddress)) {
                change
            } else {
                change.withAddress(myAddress)
            }

        putChangeToCompletableFutureMap(enrichedChange, cf)

        GlobalScope.launch(MDCContext()) {
            performProtocol(enrichedChange)
        }

        return cf
    }

    fun getPeersFromChange(change: Change): List<List<PeerAddress>> {
        if (change.peers.isEmpty()) throw RuntimeException("Change without peers")
        val peerResolver = getPeerResolver()
        return change.peers.map { peer ->
            val peersetId = peerResolver.findPeersetWithPeer(peer)
            if (peersetId == null) {
                getLogger().error("Peer $peer not found in ${peerResolver.getPeers()}")
            }
            return@map peerResolver.getPeersFromPeerset(peersetId!!) // TODO we need to specify peersetIds instead of peers
        }.map { peerset -> peerset.filter { it.globalPeerId != peerResolver.currentPeerAddress().globalPeerId } }
    }
}
