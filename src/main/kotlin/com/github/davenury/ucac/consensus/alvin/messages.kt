package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.consensus.raft.domain.LedgerItemDto

data class AlvinPropose(val peerId: Int, val entry: AlvinEntry)
data class AlvinAckPropose(val newDeps: List<HistoryEntry>, val newPos: Int)
data class AlvinAccept(val peerId: Int, val entry: AlvinEntry)
data class AlvinAckAccept(val newDeps: List<HistoryEntry>, val newPos: Int)
data class AlvinStable(val peerId: Int, val entry: AlvinEntry)
data class AlvinAckStable(val peerId: Int)
data class AlvinPromise(val entry: AlvinEntry?)

data class AlvinCommit(val result: AlvinResult)

enum class AlvinResult{
    COMMIT, ABORT
}

