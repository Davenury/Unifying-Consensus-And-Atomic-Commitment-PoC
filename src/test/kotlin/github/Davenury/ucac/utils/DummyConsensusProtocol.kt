package github.davenury.ucac.utils

import github.davenury.ucac.common.Change
import github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import github.davenury.ucac.consensus.raft.domain.ConsensusResult
import github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum


object DummyConsensusProtocol : ConsensusProtocol<Change, MutableList<ChangeWithAcceptNum>> {
    private var response: ConsensusResult = ConsensusSuccess

    override fun proposeChange(change: Change, acceptNum: Int?): ConsensusResult = response


    fun setResponse(response: ConsensusResult) {
        this.response = response
    }

    override fun getState(): MutableList<ChangeWithAcceptNum> = mutableListOf()
}