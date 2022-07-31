package com.github.davenury.ucac.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory

@JsonIgnoreProperties(ignoreUnknown = true)
data class ChangeDto(val properties: Map<String, String>) {
    fun toChange(): Change =
        (properties["operation"])?.let {
            try {
                Operation.valueOf(it).toChange(this)
            } catch (ex: IllegalArgumentException) {
                logger.debug("Error while creating Change class - unknown operation $it")
                throw UnknownOperationException(it)
            }
        }
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
