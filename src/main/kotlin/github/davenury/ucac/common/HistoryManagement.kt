package github.davenury.ucac.common

import github.davenury.ucac.consensus.raft.domain.ConsensusFailure
import github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum

abstract class HistoryManagement(private val consensusProtocol: ConsensusProtocol<Change, History>) {
    open fun change(change: Change, acceptNum: Int?) =
        consensusProtocol.proposeChange(change, acceptNum)
            .let {
                when (it) {
                    ConsensusFailure -> {
                        HistoryChangeResult.HistoryChangeFailure
                    }
                    ConsensusSuccess -> {
                        HistoryChangeResult.HistoryChangeSuccess
                    }
                }
            }

    abstract fun getLastChange(): ChangeWithAcceptNum?
    abstract fun getState(): History?

    /**
     * function used to check if history can be build given another change to perform
     * */
    abstract fun canBeBuild(newChange: Change): Boolean
    abstract fun build()
}

enum class HistoryChangeResult {
    HistoryChangeSuccess, HistoryChangeFailure
}

typealias History = MutableList<ChangeWithAcceptNum>
