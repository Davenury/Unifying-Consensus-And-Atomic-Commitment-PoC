package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusFailure
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import com.github.davenury.ucac.consensus.raft.domain.ConsensusSuccess
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import org.slf4j.LoggerFactory

class InMemoryHistoryManagement(
    private val consensusProtocol: ConsensusProtocol<Change, History>
) : HistoryManagement(consensusProtocol) {
    private val historyStorage: MutableList<ChangeWithAcceptNum> = mutableListOf()

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override fun change(change: Change, acceptNum: Int?): HistoryChangeResult =
        runBlocking {
            consensusProtocol.proposeChange(change)
                .let {
                    when (it) {
                        ConsensusFailure -> {
                            HistoryChangeResult.HistoryChangeFailure
                        }
                        ConsensusSuccess -> {
                            historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
                            HistoryChangeResult.HistoryChangeSuccess
                        }
                    }
                }
        }

    override fun getLastChange(): ChangeWithAcceptNum? =
        try {
            historyStorage.last()
        } catch (ex: java.util.NoSuchElementException) {
            logger.error("History is empty!")
            null
        }

    override fun getState(): History? =
        this.historyStorage

    override fun canBeBuild(newChange: Change): Boolean = true

    override fun build() {}

    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryHistoryManagement::class.java)
    }
}