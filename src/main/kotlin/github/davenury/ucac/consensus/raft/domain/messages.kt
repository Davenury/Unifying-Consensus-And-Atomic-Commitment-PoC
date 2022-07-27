package github.davenury.ucac.consensus.raft.domain

data class ConsensusElectMe(val peerId: Int)
data class ConsensusElectedYou(val peerId: Int, val voteGranted: Boolean)
data class ConsensusImTheLeader(val peerId: Int)
