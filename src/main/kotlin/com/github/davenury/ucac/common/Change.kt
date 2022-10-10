package com.github.davenury.ucac.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.github.davenury.ucac.history.HistoryEntry
import com.github.davenury.ucac.history.InitialHistoryEntry
import com.github.davenury.ucac.history.IntermediateHistoryEntry
import com.github.davenury.ucac.objectMapper
import org.apache.ratis.thirdparty.com.google.common.base.Objects
import java.lang.IllegalArgumentException

// see https://github.com/FasterXML/jackson-databind/issues/2742#issuecomment-637708397
class Changes : ArrayList<Change> {
    constructor() : super()
    constructor(collection: List<Change>) : super(collection)
}

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
sealed class Change {
    abstract val parentId: String
    abstract val peers: List<String>

    // TODO remove
    abstract val acceptNum: Int?

    companion object {

        fun fromJson(json: String): Change {
            return objectMapper.readValue(json, Change::class.java)
        }

        fun fromHistoryEntry(entry: HistoryEntry): Change {
            if (entry == InitialHistoryEntry) {
                throw IllegalArgumentException("Initial history entry cannot be converted to a change")
            }
            return fromJson(entry.getContent())
        }
    }

    fun toHistoryEntry(): HistoryEntry {
        return IntermediateHistoryEntry(
            objectMapper.writeValueAsString(this),
            parentId,
        )
    }

    abstract fun withAddress(myAddress: String): Change
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(
    override val parentId: String,
    val from: String,
    val to: String,
    override val peers: List<String>,
    override val acceptNum: Int? = null,
) : Change() {
    override fun withAddress(myAddress: String): Change {
        return AddRelationChange(
            parentId,
            from,
            to,
            peers.toMutableList().also { it.add(myAddress) },
            acceptNum,
        )
    }

    override fun equals(other: Any?): Boolean {
        if (other !is AddRelationChange) {
            return false
        }
        return Objects.equal(parentId, other.parentId) &&
                Objects.equal(from, other.from) &&
                Objects.equal(to, other.to)
    }

    override fun hashCode(): Int {
        return Objects.hashCode(parentId, from, to)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeleteRelationChange(
    override val parentId: String,
    val from: String,
    val to: String,
    override val peers: List<String>,
    override val acceptNum: Int? = null,
) : Change() {
    override fun withAddress(myAddress: String): Change {
        return DeleteRelationChange(
            parentId,
            from,
            to,
            peers.toMutableList().also { it.add(myAddress) },
            acceptNum,
        )
    }

    override fun equals(other: Any?): Boolean {
        if (other !is DeleteRelationChange) {
            return false
        }
        return Objects.equal(parentId, other.parentId) &&
                Objects.equal(from, other.from) &&
                Objects.equal(to, other.to)
    }

    override fun hashCode(): Int {
        return Objects.hashCode(parentId, from, to)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddUserChange(
    override val parentId: String,
    val userName: String,
    override val peers: List<String>,
    override val acceptNum: Int? = null,
) : Change() {
    override fun withAddress(myAddress: String): Change {
        return AddUserChange(
            parentId,
            userName,
            peers.toMutableList().also { it.add(myAddress) },
            acceptNum,
        )
    }

    override fun equals(other: Any?): Boolean {
        if (other !is AddUserChange) {
            return false
        }
        return Objects.equal(parentId, other.parentId) &&
                Objects.equal(userName, other.userName)
    }

    override fun hashCode(): Int {
        return Objects.hashCode(parentId, userName)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddGroupChange(
    override val parentId: String,
    val groupName: String,
    override val peers: List<String>,
    override val acceptNum: Int? = null,
) : Change() {
    override fun withAddress(myAddress: String): Change {
        return AddGroupChange(
            parentId,
            groupName,
            peers.toMutableList().also { it.add(myAddress) },
            acceptNum,
        )
    }

    override fun equals(other: Any?): Boolean {
        if (other !is AddGroupChange) {
            return false
        }
        return Objects.equal(parentId, other.parentId) &&
                Objects.equal(groupName, other.groupName)
    }

    override fun hashCode(): Int {
        return Objects.hashCode(parentId, groupName)
    }
}
