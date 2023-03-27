package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.PeerId


data class AlvinPropose(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckPropose(val newDeps: List<String>, val newPos: Int)
data class AlvinAccept(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckAccept(val newDeps: List<String>, val newPos: Int)
data class AlvinStable(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckStable(val peerId: PeerId)
data class AlvinPromise(val entry: AlvinEntryDto?)

data class AlvinCommit(val entry: AlvinEntryDto, val result: AlvinResult, val peerId: PeerId)

data class AlvinFastRecovery(val entryId: String)
data class AlvinFastRecoveryResponse(val entry: AlvinEntryDto?, val historyEntry: String?)

enum class AlvinResult{
    COMMIT, ABORT
}

