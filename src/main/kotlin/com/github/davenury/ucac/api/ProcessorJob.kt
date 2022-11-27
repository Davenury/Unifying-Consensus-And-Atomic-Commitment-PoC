package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import java.util.concurrent.CompletableFuture

enum class ProcessorJobType {
    CONSENSUS, GPAC, TWO_PC;

    companion object {
        public fun getJobType(isOnePeersetChange: Boolean, enforceGpac: Boolean, useTwoPC: Boolean): ProcessorJobType =
            when {
                isOnePeersetChange && !enforceGpac -> CONSENSUS
                useTwoPC -> TWO_PC
                else -> GPAC
            }
    }
}

data class ProcessorJob(
    val change: Change,
    val completableFuture: CompletableFuture<ChangeResult>,
    val processorJobType: ProcessorJobType
) {

    constructor(
        change: Change,
        cf: CompletableFuture<ChangeResult>,
        isOnePeersetChange: Boolean,
        enforceGpac: Boolean,
        useTwoPC: Boolean
    ) : this(change, cf, ProcessorJobType.getJobType(isOnePeersetChange, enforceGpac, useTwoPC))
}