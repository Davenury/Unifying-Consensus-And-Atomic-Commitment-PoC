package com.github.davenury.ucac.common

import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory

enum class Operation {
    ADD_RELATION {
        override fun toChange(changeDto: ChangeDto): Change {
            return AddRelationChange(changeDto.properties["from"]!!, changeDto.properties["to"]!!, changeDto.peers)
        }
    },
    DELETE_RELATION {
        override fun toChange(changeDto: ChangeDto): Change {
            return DeleteRelationChange(changeDto.properties["from"]!!, changeDto.properties["to"]!!, changeDto.peers)
        }
    },
    ADD_USER {
        override fun toChange(changeDto: ChangeDto): Change {
            return AddUserChange(changeDto.properties["userName"]!!, changeDto.peers)
        }
    },
    ADD_GROUP {
        override fun toChange(changeDto: ChangeDto): Change {
            return AddGroupChange(changeDto.properties["groupName"]!!, changeDto.peers)
        }
    };

    // Something like this may be problematic with complex fields, but since we don't need them now, we don't need to worry about them
    abstract fun toChange(changeDto: ChangeDto): Change
}