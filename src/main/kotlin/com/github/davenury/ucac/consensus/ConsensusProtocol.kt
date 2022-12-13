package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeApplyingTransition
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Transition
import com.github.davenury.common.history.History
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {
    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        return proposeTransitionAsync(ChangeApplyingTransition(change))
    }

    suspend fun proposeTransitionAsync(transition: Transition): CompletableFuture<ChangeResult>

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?
}
