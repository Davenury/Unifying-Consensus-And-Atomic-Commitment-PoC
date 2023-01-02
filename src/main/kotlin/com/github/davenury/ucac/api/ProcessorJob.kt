package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.ProtocolName
import java.util.concurrent.CompletableFuture


data class ProcessorJob(
    val change: Change,
    val completableFuture: CompletableFuture<ChangeResult>,
    val protocolName: ProtocolName
)