package com.github.davenury.ucac.common

import com.github.davenury.ucac.consensus.raft.domain.ConsensusResult.*
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProtocol
import org.slf4j.LoggerFactory

class InMemoryHistoryManagement(
    private val consensusProtocol: ConsensusProtocol<Change, History>
) : HistoryManagement(consensusProtocol) {
    private val historyStorage: MutableList<ChangeWithAcceptNum> = mutableListOf()

    // TODO: think about what's better - if change asks consensus protocol if it
    // can be done or if something higher asks and then calls change
    override suspend fun change(change: Change, acceptNum: Int?): HistoryChangeResult =
            consensusProtocol.proposeChange(change,acceptNum)
                .let {
                    when (it) {
                        ConsensusFailure -> {
                            HistoryChangeResult.HistoryChangeFailure
                        }
                        ConsensusSuccess -> {
                            historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
                            HistoryChangeResult.HistoryChangeSuccess
                        }
                        ConsensusResultUnknown -> {
                            historyStorage.add(ChangeWithAcceptNum(change, acceptNum))
                            HistoryChangeResult.HistoryChangeSuccess
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