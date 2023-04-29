package com.github.davenury.ucac.consensus.paxos

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.consensus.LeaderBasedConsensusProtocol
import java.util.concurrent.CompletableFuture

interface PaxosProtocol : LeaderBasedConsensusProtocol {
    suspend fun handlePropose(message: PaxosPropose): PaxosPromise
    suspend fun handleAccept(message: PaxosAccept): PaxosAccepted
    suspend fun handleCommit(message: PaxosCommit)

    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>

    suspend fun <A>broadcast(message: A)
    suspend fun <A>send(message: A, toNode: PeerAddress)
}