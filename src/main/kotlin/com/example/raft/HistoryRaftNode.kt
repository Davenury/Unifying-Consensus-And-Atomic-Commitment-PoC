package com.example.raft

import com.example.domain.*
import com.example.infrastructure.RatisHistoryManagement
import com.example.objectMapper
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File

class HistoryRaftNode(peerId: Int) :
    RaftNode(peerId, HistoryStateMachine(), File("./history-$peerId")),
    Closeable,
    ConsensusProtocol<Change, History> {

    override fun proposeChange(change: Change): ConsensusResult {
        val msg = objectMapper.writeValueAsString(change)
        val result = applyTransaction(msg)
        return if (result == "INVALID_OPERATION") ConsensusFailure else ConsensusSuccess
    }

    override fun getState(): History? {
        val msg = HistoryStateMachine.OperationType.STATE.toString()
        val result = queryData(msg)
        return try {
            objectMapper
                .readValue(result, mutableListOf<LinkedHashMap<String, String>>().javaClass)
                .map { ChangeDto(it).toChange() }
                .toMutableList()
        } catch (e: Exception) {
            logger.error("Can't parse result from state machine \n ${e.message}")
            null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RatisHistoryManagement::class.java)
    }
}