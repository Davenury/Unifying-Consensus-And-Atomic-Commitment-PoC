package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult
import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.history.History
import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class HistoryRaftNode(peerId: Int, peersetId: Int, constants: RaftConfiguration) :
    RaftNode(peerId, HistoryStateMachine(), File("./history-$peerId-$peersetId-${UUID.randomUUID()}"), constants),
    ConsensusProtocol<Change, History> {

    override suspend fun proposeChange(change: Change): ConsensusResult {
        val msg = objectMapper.writeValueAsString(change)
        val result = applyTransaction(msg)
        return if (result == "INVALID_OPERATION") ConsensusFailure else ConsensusSuccess
    }

    override fun getState(): History {
        val msg = HistoryStateMachine.OperationType.STATE.toString()
        val result = queryData(msg)
        val changes = objectMapper.readValue(result, Changes::class.java)
        return changes.toHistory()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}
