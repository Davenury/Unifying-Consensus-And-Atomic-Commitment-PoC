package com.github.davenury.ucac.consensus.alvin


data class AlvinPropose(val peerId: Int, val entry: AlvinEntryDto)
data class AlvinAckPropose(val newDeps: List<String>, val newPos: Int)
data class AlvinAccept(val peerId: Int, val entry: AlvinEntryDto)
data class AlvinAckAccept(val newDeps: List<String>, val newPos: Int)
data class AlvinStable(val peerId: Int, val entry: AlvinEntryDto)
data class AlvinAckStable(val peerId: Int)
data class AlvinPromise(val entry: AlvinEntryDto?)

data class AlvinCommit(val entry: AlvinEntryDto, val result: AlvinResult, val peerId: Int)

data class AlvinFastRecovery(val entryId: String)
data class AlvinFastRecoveryResponse(val entry: AlvinEntryDto?, val historyEntry: String?)

enum class AlvinResult{
    COMMIT, ABORT
}

