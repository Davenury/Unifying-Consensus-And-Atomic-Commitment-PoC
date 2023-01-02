package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change


data class ConsensusElectMe(val peerId: Int, val term: Int, val lastLogIndex: Int)

data class ConsensusElectedYou(val peerId: Int, val myTerm: Int, val voteGranted: Boolean)

data class ConsensusImTheLeader(val peerId: Int, val peerAddress: String, val leaderIteration: Int)


data class ConsensusHeartbeat(
    val leaderId: Int,
    val term: Int,
    val acceptedChanges: List<LedgerItemDto>,
    val proposedChanges: List<LedgerItemDto>,
    val prevLogIndex: Int?,
    val prevLogTerm: Int?
)

data class ConsensusHeartbeatResponse(
    val success: Boolean,
    val term: Int,
    val transactionBlocked: Boolean = false
)

typealias ConsensusProposeChange = Change
