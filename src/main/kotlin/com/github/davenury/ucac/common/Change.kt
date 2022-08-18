package com.github.davenury.ucac.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory

typealias History = MutableList<ChangeWithAcceptNum>

data class HistoryDto(
    val changes: List<ChangeWithAcceptNumDto>
)

fun History.toDto() =
    HistoryDto(changes = this.map { it.toDto() })

data class ChangeWithAcceptNum(val change: Change, val acceptNum: Int?) {
    companion object {
        fun fromJson(json: String): ChangeWithAcceptNum {
            val map = objectMapper.readValue(json, HashMap<String, Any>().javaClass)
            val changeString = objectMapper.writeValueAsString(map["change"])
            val change: Change = Change.fromJson(changeString)!!
            val acceptNum = map["acceptNum"] as Int?
            return ChangeWithAcceptNum(change, acceptNum)
        }
    }

    fun toDto(): ChangeWithAcceptNumDto = ChangeWithAcceptNumDto(change.toDto(), acceptNum)
}

data class ChangeWithAcceptNumDto(val changeDto: ChangeDto, val acceptNum: Int?) {

    fun toChangeWithAcceptNum(): ChangeWithAcceptNum = ChangeWithAcceptNum(changeDto.toChange(), acceptNum)
}


@JsonIgnoreProperties(ignoreUnknown = true)
data class ChangeDto(val properties: Map<String, String>) {
    fun toChange(): Change =
        (properties["operation"])
            ?.let { Operation.valueOf(it).toChange(this) }
            ?: throw MissingParameterException("\"operation\" value is required!")

    companion object {
        private val logger = LoggerFactory.getLogger(ChangeDto::class.java)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
sealed class Change(val operation: Operation) {
    abstract fun toDto(): ChangeDto

    companion object {

        fun fromJson(json: String): Change? =
            objectMapper
                .readValue(json, HashMap<String, String>().javaClass)
                ?.let { ChangeDto(it) }
                ?.toChange()
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(val from: String, val to: String) : Change(Operation.ADD_RELATION) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "from" to from, "to" to to))

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeleteRelationChange(val from: String, val to: String) :
    Change(Operation.DELETE_RELATION) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "from" to from, "to" to to))

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddUserChange(val userName: String) : Change(Operation.ADD_USER) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "userName" to userName))

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddGroupChange(val groupName: String) : Change(Operation.ADD_GROUP) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "groupName" to groupName))

}
