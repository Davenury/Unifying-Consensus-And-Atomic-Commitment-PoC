package github.davenury.ucac.consensus.raft.infrastructure

import github.davenury.ucac.common.Change
import github.davenury.ucac.common.History
import github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import github.davenury.ucac.consensus.raft.domain.ConsensusResult
import github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum

class DummyConsensusProtocol : ConsensusProtocol<Change, History> {
    private val historyStorage: History = mutableListOf()

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult {
        historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
        return ConsensusSuccess
    }

    override fun getState(): History = historyStorage
}