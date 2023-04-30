package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.History
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import java.io.File
import java.util.*
import java.util.concurrent.CompletableFuture

class HistoryRatisNode(
    peerId: Int,
    private val peersetId: PeersetId,
    private val peerResolver: PeerResolver,
    private val history: History,
) :
    RatisNode(
        peerId,
        HistoryStateMachine(history),
        File("./history-$peerId-$peersetId-${UUID.randomUUID()}"),
        peersetId,
        peerResolver,
    ),
    ConsensusProtocol {

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    override fun amILeader(): Boolean = true

    override fun getLeaderId(): PeerId? = peerResolver.currentPeer()

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        val cf = CompletableFuture<ChangeResult>()
        val changeId = change.id
        changeIdToCompletableFuture[changeId] = cf

        GlobalScope.launch(MDCContext()) {
            val result = applyTransaction(change.toHistoryEntry(peersetId).serialize())
            val changeResult = if (result == "ERROR") {
                ChangeResult(ChangeResult.Status.CONFLICT)
            } else {
                ChangeResult(ChangeResult.Status.SUCCESS)
            }
            changeIdToCompletableFuture[changeId]!!.complete(changeResult)
        }
        return cf
    }

    override fun getState(): History {
        return history
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? =
        changeIdToCompletableFuture[changeId]
}
