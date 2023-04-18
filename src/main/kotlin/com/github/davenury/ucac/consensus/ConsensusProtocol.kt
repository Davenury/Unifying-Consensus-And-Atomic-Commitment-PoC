package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    suspend fun begin() {}

    suspend fun proposeChangeAsync(change: Change, redirectIfNotLeader: Boolean = false): CompletableFuture<ChangeResult>

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    fun stop() {

    }
}
