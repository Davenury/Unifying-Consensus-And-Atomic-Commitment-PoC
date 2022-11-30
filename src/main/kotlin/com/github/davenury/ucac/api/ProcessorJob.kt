package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import java.util.concurrent.CompletableFuture

enum class ProcessorJobType {
    CONSENSUS, GPAC, TWO_PC;
}

data class ProcessorJob(
    val change: Change,
    val completableFuture: CompletableFuture<ChangeResult>,
    val processorJobType: ProcessorJobType
)