package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.history.History
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.common.ProtocolTimerImpl
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import java.util.concurrent.CompletableFuture

interface ConsensusProtocol {

    suspend fun begin()
    fun setPeerAddress(address: String)
    fun stop()
    @Deprecated("use proposeChangeAsync")
    suspend fun proposeChange(change: Change): ChangeResult

    suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult>

    suspend fun proposeChangeToLedger(result: CompletableFuture<ChangeResult>, change: Change)

    fun getState(): History

    fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>?

    fun isMoreThanHalf(value: Int): Boolean = (value + 1) * 2 > otherConsensusPeers().size + 1


    fun otherConsensusPeers(): List<PeerAddress>

    suspend fun getProposedChanges(): List<Change>
    suspend fun getAcceptedChanges(): List<Change>
}
