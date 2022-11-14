package com.github.davenury.common

import java.lang.Exception

data class ChangeResult(
    val status: Status,
    val exception: Exception? = null
) {
    enum class Status {
        /**
         * Change accepted and applied
         */
        SUCCESS,

        /**
         * Change not applied due to another change conflicting with it.
         */
        CONFLICT,

        /**
         * Change not applied due to a timeout.
         * For instance, there was not enough peers to accept the change
         * within the time limit.
         */
        TIMEOUT,

        /**
         * Processing change failed with exception.
         */
        EXCEPTION,
    }
}
