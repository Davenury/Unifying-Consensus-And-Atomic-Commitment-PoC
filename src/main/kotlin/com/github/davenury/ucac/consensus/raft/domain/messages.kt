package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.common.Change


data class ConsensusElectMe(val peerId: Int, val leaderIteration: Int, val lastAcceptedId: Int)

data class ConsensusElectedYou(val peerId: Int, val myIteration: Int, val voteGranted: Boolean)

data class ConsensusImTheLeader(val peerId: Int, val peerAddress: String, val leaderIteration: Int)


data class ConsensusHeartbeat(
    val peerId: Int,
    val iteration: Int,
    val acceptedChanges: List<LedgerItemDto>,
    val proposedChanges: List<LedgerItemDto>
)

data class ConsensusHeartbeatResponse(
    val accepted: Boolean,
    val leaderAddress: String?
)

typealias ConsensusProposeChange = Change
