package com.github.davenury.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonProcessingException
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.history.IntermediateHistoryEntry
import java.util.*


// see https://github.com/FasterXML/jackson-databind/issues/2742#issuecomment-637708397
class Changes : ArrayList<Change> {
    constructor() : super()

    constructor(collection: List<Change>) : super(collection)

    companion object {
        fun fromHistory(history: History): Changes {
            return history.toEntryList()
                .reversed()
                .mapNotNull { Change.fromHistoryEntry(it) }
                .let { Changes(it) }
        }
    }
}

data class ChangePeersetInfo(
    val peersetId: Int,
    val parentId: String?,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = AddRelationChange::class, name = "ADD_RELATION"),
        JsonSubTypes.Type(value = DeleteRelationChange::class, name = "DELETE_RELATION"),
        JsonSubTypes.Type(value = AddUserChange::class, name = "ADD_USER"),
        JsonSubTypes.Type(value = AddGroupChange::class, name = "ADD_GROUP"),
        JsonSubTypes.Type(value = TwoPCChange::class, name = "TWO_PC_Change"),
    )
)
sealed class Change {
    val id = UUID.randomUUID().toString()
    abstract val peersets: List<ChangePeersetInfo>
    abstract val notificationUrl: String?

    // TODO remove
    abstract val acceptNum: Int?

    fun getPeersetInfo(peersetId: Int): ChangePeersetInfo? =
        peersets.find { it.peersetId == peersetId }

    fun toHistoryEntry(peersetId: Int, parentIdDefault: String? = null): HistoryEntry {
        val info = getPeersetInfo(peersetId)
            ?: throw IllegalArgumentException("Unknown peersetId: $peersetId")
        return IntermediateHistoryEntry(
            objectMapper.writeValueAsString(this),
            info.parentId
                ?: parentIdDefault
                ?: throw IllegalArgumentException("No parent ID"),
        )
    }

    abstract fun copyWithNewParentId(peersetId: Int, parentId: String?): Change

    protected fun doesEqual(other: Any?): Boolean =
        (other is Change) && Objects.equals(id, other.id)

    companion object {
        private fun fromJson(json: String): Change =  objectMapper.readValue(json, Change::class.java)

        fun fromHistoryEntry(entry: HistoryEntry): Change? {
            if (entry == InitialHistoryEntry) {
                return null
            }

            return try {
                fromJson(entry.getContent())
            } catch (e: JsonProcessingException) {
                null
            }
        }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(
    override val peersets: List<ChangePeersetInfo>,
    val from: String,
    val to: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
) : Change() {
    override fun equals(other: Any?): Boolean {
        if (other !is AddRelationChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) &&
                Objects.equals(from, other.from) &&
                Objects.equals(to, other.to)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, from, to)
    }

    override fun copyWithNewParentId(peersetId: Int, parentId: String?): Change =
        this.copy(peersets = peersets.map {
            if (it.peersetId == peersetId) {
                ChangePeersetInfo(peersetId, parentId)
            } else {
                it
            }
        })
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeleteRelationChange(
    override val peersets: List<ChangePeersetInfo>,
    val from: String,
    val to: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
) : Change() {
    override fun equals(other: Any?): Boolean {
        if (other !is DeleteRelationChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) &&
                Objects.equals(from, other.from) &&
                Objects.equals(to, other.to)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, from, to)
    }

    override fun copyWithNewParentId(peersetId: Int, parentId: String?): Change =
        this.copy(peersets = peersets.map {
            if (it.peersetId == peersetId) {
                ChangePeersetInfo(peersetId, parentId)
            } else {
                it
            }
        })
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddUserChange(
    override val peersets: List<ChangePeersetInfo>,
    val userName: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
) : Change() {
    override fun equals(other: Any?): Boolean {
        if (other !is AddUserChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) &&
                Objects.equals(userName, other.userName)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, userName)
    }

    override fun copyWithNewParentId(peersetId: Int, parentId: String?): Change =
        this.copy(peersets = peersets.map {
            if (it.peersetId == peersetId) {
                ChangePeersetInfo(peersetId, parentId)
            } else {
                it
            }
        })
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddGroupChange(
    override val peersets: List<ChangePeersetInfo>,
    val groupName: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
) : Change() {
    override fun equals(other: Any?): Boolean {
        if (other !is AddGroupChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) &&
                Objects.equals(groupName, other.groupName)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, groupName)
    }

    override fun copyWithNewParentId(peersetId: Int, parentId: String?): Change =
        this.copy(peersets = peersets.map {
            if (it.peersetId == peersetId) {
                ChangePeersetInfo(peersetId, parentId)
            } else {
                it
            }
        })
}


enum class TwoPCStatus {
    ACCEPTED,
    ABORTED
}

// TwoPC should always contain two changes:
// If accepted: 2PCChange-Accept -> Change
// Else: 2PCChange-Accept -> 2PCChange-Abort
@JsonIgnoreProperties(ignoreUnknown = true)
data class TwoPCChange(
    override val peersets: List<ChangePeersetInfo>,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    val twoPCStatus: TwoPCStatus,
    val change: Change,
) : Change() {
    override fun equals(other: Any?): Boolean {
        if (other !is TwoPCChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) &&
                Objects.equals(twoPCStatus, other.twoPCStatus) &&
                Objects.equals(this.change, other.change)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, twoPCStatus)
    }

    override fun copyWithNewParentId(peersetId: Int, parentId: String?): Change =
        this.copy(peersets = peersets.map {
            if (it.peersetId == peersetId) {
                ChangePeersetInfo(peersetId, parentId)
            } else {
                it
            }
        })
}
