package com.example.consensus.raft.infrastructure

import com.example.common.Change
import com.example.common.History
import com.example.consensus.raft.domain.ConsensusProtocol
import com.example.consensus.raft.domain.ConsensusResult
import com.example.consensus.raft.domain.ConsensusSuccess
import com.example.consensus.ratis.ChangeWithAcceptNum
import java.time.Duration

/**
 * @author Kamil Jarosz
 */
class RaftConsensusProtocolImpl(peerId: Int, peersetId: Int) :
    ConsensusProtocol<Change, History> {

    private val voteGranted: Map<PeerId, ConsensusPeer> = mutableMapOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
        return ConsensusSuccess
    }

    override fun getState(): History? {
        // TODO
        return emptyList<ChangeWithAcceptNum>() as History
    }
}

data class PeerId(val id: Int)
data class ConsensusPeer(
    val peerId: PeerId,
    val voteGranted: Boolean,
    val rpcDue: Duration,
    val heartbeatDue: Duration,
    val matchIndex: Int,
    // maybe optional
    val nextIndex: Int
)
