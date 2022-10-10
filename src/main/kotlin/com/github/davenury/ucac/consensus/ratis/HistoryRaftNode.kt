package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusFailure
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.ConsensusSuccess
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.history.HistoryEntry
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class HistoryRaftNode(private val history: History, peerId: Int, peersetId: Int, constants: RaftConfiguration) :
    RaftNode(peerId, HistoryStateMachine(history), File("./history-$peerId-$peersetId-${UUID.randomUUID()}"), constants),
    ConsensusProtocol {

    override suspend fun proposeChange(entry: HistoryEntry): ConsensusResult {
        val result = applyTransaction(entry.serialize())
        return if (result == "INVALID_OPERATION") ConsensusFailure else ConsensusSuccess
    }

    override fun getState(): History = history

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}
