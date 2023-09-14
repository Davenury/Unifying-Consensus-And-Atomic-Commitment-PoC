package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import java.util.concurrent.CompletableFuture

interface AlvinBroadcastProtocol : ConsensusProtocol {
    suspend fun handleProposalPhase(message: AlvinPropose): AlvinAckPropose
    suspend fun handleAcceptPhase(message: AlvinAccept): AlvinAckAccept
    suspend fun handleStable(message: AlvinStable): AlvinAckStable
    suspend fun handlePrepare(message: AlvinAccept): AlvinPromise
    suspend fun handleCommit(message: AlvinCommit): AlvinCommitResponse
    suspend fun handleFastRecovery(message: AlvinFastRecovery): AlvinFastRecoveryResponse
}