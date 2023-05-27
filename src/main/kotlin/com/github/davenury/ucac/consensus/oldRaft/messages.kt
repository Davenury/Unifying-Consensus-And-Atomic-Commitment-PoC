package com.github.davenury.ucac.consensus.oldRaft

import com.github.davenury.common.Change
import com.github.davenury.common.PeerId


data class ConsensusElectMe(val peerId: PeerId, val term: Int, val lastEntryId: String)

data class ConsensusElectedYou(val peerId: PeerId, val myTerm: Int, val voteGranted: Boolean)
data class ConsensusHeartbeat(
    val leaderId: PeerId,
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
    val incompatibleWithHistory: Boolean = false,
    val missingValues: Boolean = false,
    val lastCommittedEntryId: String? = null
)

typealias ConsensusProposeChange = Change
