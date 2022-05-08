package com.example

import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.HistoryManagement
import com.example.infrastructure.DummyConsensusProtocol
import com.example.infrastructure.InMemoryHistoryManagement
import com.example.infrastructure.RatisHistoryManagement
import com.example.raft.HistoryRaftNode
import com.example.raft.RaftNode
import org.koin.dsl.module

fun historyManagementModule(id: Int) = module {
    single<ConsensusProtocol<Change, MutableList<Change>>> { DummyConsensusProtocol() }
    single<RaftNode> { HistoryRaftNode(id) }
    single<HistoryManagement> { RatisHistoryManagement(get()) }
}
