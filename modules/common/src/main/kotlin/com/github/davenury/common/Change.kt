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
                .mapNotNull { Transition.fromHistoryEntry(it) }
                .mapNotNull { it as? ChangeApplyingTransition }
                .map { it.change }
                .let { Changes(it) }
        }
    }
}

class Transitions : ArrayList<Transition> {
    constructor() : super()

    constructor(collection: List<Transition>) : super(collection)

    companion object {
        fun fromHistory(history: History): Transitions {
            return history.toEntryList()
                .reversed()
                .mapNotNull { Transition.fromHistoryEntry(it) }
                .let { Transitions(it) }
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
    )
)
sealed class Change(open val id: String = UUID.randomUUID().toString()) {
    abstract val peersets: List<ChangePeersetInfo>
    abstract val notificationUrl: String?

    // TODO remove
    abstract val acceptNum: Int?

    fun getPeersetInfo(peersetId: Int): ChangePeersetInfo? =
        peersets.find { it.peersetId == peersetId }

    abstract fun copyWithNewParentId(peersetId: Int, parentId: String?): Change

    protected fun doesEqual(other: Any?): Boolean =
        (other is Change) && Objects.equals(id, other.id)
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(
    val from: String,
    val to: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo> = listOf()
) : Change(id) {

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
    val from: String,
    val to: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo> = listOf()
) : Change(id) {

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
    val userName: String,
    override val acceptNum: Int? = null,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo> = listOf(),
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null
) : Change(id) {
    override fun equals(other: Any?): Boolean {
        if (other !is AddUserChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) && Objects.equals(userName, other.userName)
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
    val groupName: String,
    override val acceptNum: Int? = null,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo>,
) : Change(id) {

    override fun equals(other: Any?): Boolean {
        if (other !is AddGroupChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) && Objects.equals(groupName, other.groupName)
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

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = ChangeApplyingTransition::class, name = "CHANGE"),
        JsonSubTypes.Type(value = TwoPCTransition::class, name = "2PC"),
    )
)
sealed class Transition(
    open val id: String = UUID.randomUUID().toString(),
    open val change: Change,
) {
    fun toHistoryEntry(peersetId: Int, parentIdDefault: String? = null): HistoryEntry {
        val info = change.getPeersetInfo(peersetId)
            ?: throw IllegalArgumentException("Unknown peersetId: $peersetId")
        return toHistoryEntry(
            info.parentId
                ?: parentIdDefault
                ?: throw IllegalArgumentException("No parent ID")
        )
    }

    protected fun toHistoryEntry(parentId: String): HistoryEntry {
        return IntermediateHistoryEntry(
            objectMapper.writeValueAsString(this),
            parentId,
        )
    }

    companion object {
        private fun fromJson(json: String): Transition {
            return objectMapper.readValue(json, Transition::class.java)
        }

        fun fromHistoryEntry(entry: HistoryEntry): Transition? {
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
data class ChangeApplyingTransition(
    override val change: Change,
    override val id: String = UUID.randomUUID().toString(),
) : Transition(id, change) {
    override fun equals(other: Any?): Boolean {
        if (other !is ChangeApplyingTransition) {
            return false
        }
        return Objects.equals(change, other.change)
    }

    override fun hashCode(): Int {
        return Objects.hash(change)
    }
}

enum class TwoPCStatus {
    ACCEPTED,
    ABORTED,
}

// TwoPC should always contain two changes:
// If accepted: 2PCChange-Accept -> Change
// Else: 2PCChange-Accept -> 2PCChange-Abort
@JsonIgnoreProperties(ignoreUnknown = true)
data class TwoPCTransition(
    override val change: Change,
    val twoPCStatus: TwoPCStatus,
    override val id: String = UUID.randomUUID().toString(),
) : Transition(id, change) {
    override fun equals(other: Any?): Boolean {
        if (other !is TwoPCTransition) {
            return false
        }
        return Objects.equals(change, other.change) &&
                Objects.equals(twoPCStatus, other.twoPCStatus)
    }

    override fun hashCode(): Int {
        return Objects.hash(change, twoPCStatus)
    }
}
