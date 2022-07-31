package com.github.davenury.ucac.common

import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory

enum class Operation {
    ADD_RELATION {
        override fun toChange(changeDto: ChangeDto): Change =
            readChange<AddRelationChange>(changeDto)
    },
    DELETE_RELATION {
        override fun toChange(changeDto: ChangeDto): Change =
            readChange<DeleteRelationChange>(changeDto)
    },
    ADD_USER {
        override fun toChange(changeDto: ChangeDto): Change =
            readChange<AddUserChange>(changeDto)
    },
    ADD_GROUP {
        override fun toChange(changeDto: ChangeDto): Change =
            readChange<AddGroupChange>(changeDto)
    };

    companion object {
        private val logger = LoggerFactory.getLogger(Operation::class.java)

        private inline fun <reified T : Change> readChange(changeDto: ChangeDto): Change =
            try {
                objectMapper.convertValue(changeDto.properties, T::class.java)
            } catch (ex: Exception) {
                logger.debug("Missing parameter exception while creating add relation change: ${ex.message}")
                throw MissingParameterException(ex.message)
            }
    }

    abstract fun toChange(changeDto: ChangeDto): Change
}