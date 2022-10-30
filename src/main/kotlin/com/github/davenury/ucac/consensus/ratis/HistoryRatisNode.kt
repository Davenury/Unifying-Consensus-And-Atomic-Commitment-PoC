package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.RatisConfig
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusFailure
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class HistoryRatisNode(
    peerId: Int,
    peersetId: Int,
    config: RatisConfig,
    private val history: History,
) :
    RatisNode(
        peerId,
        HistoryStateMachine(history),
        File("./history-$peerId-$peersetId-${UUID.randomUUID()}"),
        peersetId,
        config,
    ),
    ConsensusProtocol<Change, History> {

    override suspend fun proposeChange(change: Change): ConsensusResult {
        val result = applyTransaction(change.toHistoryEntry().serialize())
        return if (result == "ERROR") ConsensusFailure else ConsensusSuccess
    }

    override fun getState(): History {
        return history
    }
}
