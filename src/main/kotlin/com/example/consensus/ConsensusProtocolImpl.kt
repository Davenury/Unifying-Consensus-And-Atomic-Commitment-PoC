package com.example.consensus

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.ConsensusResult
import com.example.domain.History
import java.time.Duration

/**
 * @author Kamil Jarosz
 */
class ConsensusProtocolImpl(peerId: Int, peersetId: Int) :
    ConsensusProtocol<Change, History> {

    private val voteGranted: Map<PeerId, ConsensusPeer> = mutableMapOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        // TODO
    }

    override fun getState(): History? {
        // TODO
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
