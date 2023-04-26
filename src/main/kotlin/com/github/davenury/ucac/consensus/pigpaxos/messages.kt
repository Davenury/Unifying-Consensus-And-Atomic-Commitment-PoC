package com.github.davenury.ucac.consensus.pigpaxos

import com.github.davenury.common.PeerId
import com.github.davenury.common.history.HistoryEntry


data class PaxosPropose(val peerId: PeerId, val paxosRound: Int, val lastEntryId: String)

data class PaxosPromise(
    val promised: Boolean,
    override val currentRound: Int,
    override val currentLeaderId: PeerId?,
    val committedEntries: List<String>,
    val notFinishedEntries: List<String>
) : PaxosResponse(promised, currentRound, currentLeaderId)

data class PaxosAccept(val entry: String, val paxosRound: Int, val proposer: PeerId)

data class PaxosAccepted(val accepted: Boolean, override val currentRound: Int, override val currentLeaderId: PeerId?) :
    PaxosResponse(accepted, currentRound, currentLeaderId)

data class PaxosCommit(val paxosResult: PaxosResult, val entry: String, val paxosRound: Int, val proposer: PeerId)

open class PaxosResponse(val result: Boolean, open val currentRound: Int, open val currentLeaderId: PeerId?)

enum class PaxosResult {
    COMMIT, ABORT
}


fun List<HistoryEntry>.serialize(): List<String> = this.map { it.serialize() }
fun List<String>.deserialize(): List<HistoryEntry> = this.map { HistoryEntry.deserialize(it) }