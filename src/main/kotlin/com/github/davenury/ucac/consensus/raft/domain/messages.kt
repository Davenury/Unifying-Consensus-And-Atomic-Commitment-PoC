package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.common.Change


data class ConsensusElectMe(val peerId: Int, val term: Int, val lastEntryId: String)

data class ConsensusElectedYou(val peerId: Int, val myTerm: Int, val voteGranted: Boolean)
data class ConsensusHeartbeat(
    val leaderId: Int,
    val term: Int,
    val logEntries: List<LedgerItemDto>,
    val prevEntryId: String?,
    val currentHistoryEntryId: String,
    val leaderCommitId: String
)

data class ConsensusHeartbeatResponse(
    val success: Boolean,
    val term: Int,
    val transactionBlocked: Boolean = false,
    val incompatibleWithHistory: Boolean = false
)

typealias ConsensusProposeChange = Change
