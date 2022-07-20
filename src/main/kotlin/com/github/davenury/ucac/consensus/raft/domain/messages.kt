package com.github.davenury.ucac.consensus.raft.domain

import com.example.common.ChangeWithAcceptNumDto


data class ConsensusElectMe(val peerId: Int, val leaderIteration: Int)

data class ConsensusElectedYou(val peerId: Int, val voteGranted: Boolean)

data class ConsensusImTheLeader(val peerId: Int, val peerAddress: String, val leaderIteration: Int)


data class ConsensusHeartbeat(
    val peerId: Int,
    val acceptedChanges: List<LedgerItemDto>,
    val proposedChanges: List<LedgerItemDto>
)

data class ConsensusProposeChange(val change: ChangeWithAcceptNumDto)
