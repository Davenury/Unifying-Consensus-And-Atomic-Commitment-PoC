package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.PeerId


data class AlvinPropose(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckPropose(val newDeps: List<String>, val newPos: Int)
data class AlvinAccept(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckAccept(val newDeps: List<String>, val newPos: Int)
data class AlvinStable(val peerId: PeerId, val entry: AlvinEntryDto)
data class AlvinAckStable(val peerId: PeerId)
data class AlvinPromise(val entry: AlvinEntryDto?, val isFinished: Boolean)

data class AlvinCommit(val entry: AlvinEntryDto, val result: AlvinResult, val peerId: PeerId)
data class AlvinCommitResponse(val result: AlvinResult?, val peerId: PeerId)

data class AlvinFastRecovery(val askedEntryId: String, val currentEntryId: String, val peerId: PeerId)
data class AlvinFastRecoveryResponse(val entries: List<AlvinEntryDto?>, val historyEntries: List<String>, val isFinished: Boolean)

enum class AlvinResult{
    COMMIT, ABORT
}

