package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    @Deprecated("use proposeChangeAsync")
    suspend fun proposeChange(change: Change): ChangeResult

    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change)

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?
}
