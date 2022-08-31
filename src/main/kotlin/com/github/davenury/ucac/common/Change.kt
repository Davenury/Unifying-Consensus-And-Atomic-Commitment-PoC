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
            val change: Change = Change.fromJson(changeString)
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
// peers contains one entry for each peerset
data class ChangeDto(val properties: Map<String, String>, val peers: List<List<String>>) {
    fun toChange(): Change =
        (properties["operation"])
            ?.let {
                try {
                    Operation.valueOf(it).toChange(this)
                } catch (ex: IllegalArgumentException) {
                    logger.error("Error while creating Change class - unknown operation $it")
                    throw UnknownOperationException(it)
                }
            } ?: throw MissingParameterException("\"operation\" value is required!")

    companion object {
        private val logger = LoggerFactory.getLogger(ChangeDto::class.java)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
sealed class Change(val operation: Operation) {
    abstract fun toDto(): ChangeDto
    abstract val peers: List<List<String>>

    companion object {

        fun fromJson(json: String): Change {
            val properties = objectMapper
                .readValue(json, HashMap<String, Any>()::class.java)

            val peers = properties["peers"] as List<List<String>>
            val otherProperties = properties.filter { (key, _) -> key != "peers" } as Map<String, String>

            return ChangeDto(otherProperties, peers).toChange()
        }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(val from: String, val to: String, override val peers: List<List<String>>) : Change(Operation.ADD_RELATION) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "from" to from, "to" to to), peers)

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeleteRelationChange(val from: String, val to: String, override val peers: List<List<String>>) :
    Change(Operation.DELETE_RELATION) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "from" to from, "to" to to), peers)

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddUserChange(val userName: String, override val peers: List<List<String>>) : Change(Operation.ADD_USER) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "userName" to userName), peers)

}

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddGroupChange(val groupName: String, override val peers: List<List<String>>) : Change(Operation.ADD_GROUP) {
    override fun toDto(): ChangeDto =
        ChangeDto(mapOf("operation" to this.operation.name, "groupName" to groupName), peers)

}
