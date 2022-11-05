package com.github.davenury.ucac.consensus

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import com.github.davenury.ucac.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    @Deprecated("use proposeChangeAsync")
    suspend fun proposeChange(change: Change): ChangeResult

    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?
}
