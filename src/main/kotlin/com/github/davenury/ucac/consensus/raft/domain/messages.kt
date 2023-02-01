package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change
import com.github.davenury.ucac.common.PeerAddress


data class ConsensusElectMe(val peerId: Int, val term: Int, val lastLogIndex: Int)

data class ConsensusElectedYou(val peerId: Int, val myTerm: Int, val voteGranted: Boolean)

// FIXME: Remove acceptedChanges, add leaderCommit
data class ConsensusHeartbeat(
    val leaderId: Int,
    val term: Int,
    val acceptedChanges: List<LedgerItemDto>,
    val proposedChanges: List<LedgerItemDto>,
    val prevLogIndex: Int?,
    val prevLogTerm: Int?,
    val currentHistoryEntryId: String
)

data class ConsensusHeartbeatResponse(
    val success: Boolean,
    val term: Int,
    val transactionBlocked: Boolean = false,
    val incompatibleWithHistory: Boolean = false
)

typealias ConsensusProposeChange = Change
