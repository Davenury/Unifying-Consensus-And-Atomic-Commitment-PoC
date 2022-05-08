package com.example.domain

import com.example.objectMapper
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.slf4j.LoggerFactory

data class ChangeDto(
    val properties: Map<String, String>
) {
    fun toChange(): Change =
        (properties["operation"])?.let {
            try {
                Operation.valueOf(it).toChange(this)
            } catch (ex: IllegalArgumentException) {
                logger.debug("Error while creating Change class - unknown operation $it")
                throw UnknownOperationException(it)
            }
        } ?: throw MissingParameterException("\"operation\" value is required!")

    companion object {
        private val logger = LoggerFactory.getLogger(ChangeDto::class.java)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
sealed class Change(
    val operation: Operation
) {
    companion object {

        fun fromJson(json: String): Change = objectMapper
            .readValue(json, HashMap<String, String>().javaClass)
            .let { ChangeDto(it) }.toChange()
    }
}


@JsonIgnoreProperties(ignoreUnknown = true)
data class AddRelationChange(val from: String, val to: String) : Change(Operation.ADD_RELATION)

@JsonIgnoreProperties(ignoreUnknown = true)
data class DeleteRelationChange(val from: String, val to: String) : Change(Operation.DELETE_RELATION)

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddUserChange(val userName: String) : Change(Operation.ADD_USER)

@JsonIgnoreProperties(ignoreUnknown = true)
data class AddGroupChange(val groupName: String) : Change(Operation.ADD_GROUP)