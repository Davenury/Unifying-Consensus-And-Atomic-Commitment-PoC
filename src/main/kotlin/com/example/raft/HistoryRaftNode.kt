package com.example.raft

import com.example.domain.*
import com.example.infrastructure.RatisHistoryManagement
import com.example.objectMapper
import java.io.File
import org.slf4j.LoggerFactory

class HistoryRaftNode(peerId: Int, peersetId: Int) :
        RaftNode(peerId, HistoryStateMachine(), File("./history-$peerId-$peersetId")),
        ConsensusProtocol<Change, History> {

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        val msg = objectMapper.writeValueAsString(ChangeWithAcceptNum(change, acceptNum))
        val result = applyTransaction(msg)
        return if (result == "INVALID_OPERATION") ConsensusFailure else ConsensusSuccess
    }

    override fun getState(): History? {
        val msg = HistoryStateMachine.OperationType.STATE.toString()
        val result = queryData(msg)
        return try {
            objectMapper
                    .readValue(result, mutableListOf<LinkedHashMap<String, Any>>().javaClass)
                    .map {
                        ChangeWithAcceptNum(
                                ChangeDto(it["change"]!! as Map<String, String>).toChange(),
                                it["acceptNum"] as Int
                        )
                    }
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
