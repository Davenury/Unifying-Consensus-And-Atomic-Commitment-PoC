package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?
}
