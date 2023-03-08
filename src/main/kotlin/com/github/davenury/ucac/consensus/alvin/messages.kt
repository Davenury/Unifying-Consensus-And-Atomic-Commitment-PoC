import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.ucac.consensus.alvin.AlvinEntry
import com.github.davenury.ucac.consensus.raft.domain.LedgerItemDto

data class AlvinPropose(val peerId: Int, val entry: AlvinEntry)
data class AlvinAckPropose(val newDeps: List<HistoryEntry>, val newPos: Int)

data class AlvinDecision(val peerId: Int, val myTerm: Int, val voteGranted: Boolean)
data class AlvinAccept(
    val leaderId: Int,
    val term: Int,
    val logEntries: List<LedgerItemDto>,
    val prevEntryId: String?,
    val currentHistoryEntryId: String,
    val leaderCommitId: String
)

data class AlvinDelivery(
    val success: Boolean,
    val term: Int,
    val transactionBlocked: Boolean = false,
    val incompatibleWithHistory: Boolean = false
)

