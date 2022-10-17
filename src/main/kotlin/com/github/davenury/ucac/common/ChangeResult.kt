package com.github.davenury.ucac.common

data class ChangeResult(
    val status: Status,
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
    }
}
