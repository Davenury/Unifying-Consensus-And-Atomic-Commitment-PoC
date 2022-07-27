package github.davenury.ucac.consensus.ratis

import github.davenury.ucac.common.Change
import github.davenury.ucac.common.ChangeDto
import github.davenury.ucac.common.History
import github.davenury.ucac.consensus.raft.domain.ConsensusFailure
import github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import github.davenury.ucac.consensus.raft.domain.ConsensusResult
import github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class HistoryRaftNode(peerId: Int, peersetId: Int, constants: RaftConfiguration) :
    RaftNode(peerId, HistoryStateMachine(), File("./history-$peerId-$peersetId-${UUID.randomUUID()}"), constants),
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
