package com.github.davenury.ucac.commitment

import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeResult
import java.util.concurrent.CompletableFuture

/**
 * @author Kamil Jarosz
 */
interface AtomicCommitmentProtocol {
    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>
}
