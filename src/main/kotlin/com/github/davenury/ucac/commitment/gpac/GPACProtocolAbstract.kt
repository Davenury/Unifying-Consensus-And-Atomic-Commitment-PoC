package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.Change
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.PeerResolver
import org.slf4j.Logger

abstract class GPACProtocolAbstract(peerResolver: PeerResolver, logger: Logger) : SignalSubject,
    AbstractAtomicCommitmentProtocol(logger, peerResolver) {

    abstract suspend fun handleElect(message: ElectMe): ElectedYou
    abstract suspend fun handleAgree(message: Agree): Agreed
    abstract suspend fun handleApply(message: Apply)

    abstract suspend fun performProtocolAsLeader(change: Change, iteration: Int = 1)
    abstract suspend fun performProtocolAsRecoveryLeader(change: Change, iteration: Int = 1)
    abstract fun getTransaction(): Transaction
    abstract fun getBallotNumber(): Int
}
