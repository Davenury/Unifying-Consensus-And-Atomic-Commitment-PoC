package com.github.davenury.ucac.commitment

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import java.util.concurrent.CompletableFuture

/**
 * @author Kamil Jarosz
 */
interface AtomicCommitmentProtocol {
    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>
}
