package com.example

import com.example.api.HistoryManagementFacade
import com.example.api.HistoryManagementFacadeImpl
import com.example.domain.Change
import com.example.domain.ConsensusProtocol
import com.example.domain.HistoryManagement
import com.example.infrastructure.DummyConsensusProtocol
import com.example.infrastructure.InMemoryHistoryManagement
import org.koin.dsl.module

val historyManagementModule = module {
    single<ConsensusProtocol<Change, MutableList<Change>>> { DummyConsensusProtocol() }
    single<HistoryManagement> { InMemoryHistoryManagement(get()) }
    single<HistoryManagementFacade> { HistoryManagementFacadeImpl(get()) }
}
