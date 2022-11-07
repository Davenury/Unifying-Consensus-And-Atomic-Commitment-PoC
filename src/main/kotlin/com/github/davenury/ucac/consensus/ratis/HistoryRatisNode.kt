package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.RatisConfig
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.history.History
import java.io.File
import java.util.*
import java.util.concurrent.CompletableFuture

class HistoryRatisNode(
    peerId: Int,
    peersetId: Int,
    config: RatisConfig,
    private val history: History,
) :
    RatisNode(
        peerId,
        HistoryStateMachine(history),
        File("./history-$peerId-$peersetId-${UUID.randomUUID()}"),
        peersetId,
        config,
    ),
    ConsensusProtocol {

    @Deprecated("use proposeChangeAsync")
    override suspend fun proposeChange(change: Change): ChangeResult {
        val result = applyTransaction(change.toHistoryEntry().serialize())
        return if (result == "ERROR") ChangeResult(ChangeResult.Status.CONFLICT) else ChangeResult(ChangeResult.Status.SUCCESS)
    }

    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        TODO("Not yet implemented")
    }

    override fun getState(): History {
        return history
    }
}
