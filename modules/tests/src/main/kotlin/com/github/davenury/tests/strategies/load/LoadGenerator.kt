package com.github.davenury.tests.strategies.load

import com.github.davenury.tests.Config

interface LoadGenerator {
    fun generate()
    suspend fun subscribe(fn: suspend () -> Unit)
    fun getName(): String

    companion object {
        fun createFromConfig(config: Config): LoadGenerator =
            when (config.loadGeneratorConfig.loadGeneratorType.lowercase()) {
                "constant" -> ConstantLoadGenerator(config.constantLoad!!)
                "bound" -> BoundLoadGenerator(
                    config.numberOfRequestsToSendToSinglePeerset!! + config.numberOfRequestsToSendToMultiplePeersets!!,
                    config.loadGeneratorConfig.timeOfSimulation!!
                )

                "increasing" -> IncreasingLoadGenerator(
                    config.loadGeneratorConfig.increasingLoadBound!!,
                    config.loadGeneratorConfig.increasingLoadIncreaseDelay!!,
                    config.loadGeneratorConfig.increasingLoadIncreaseStep!!,
                )

                else -> throw IllegalArgumentException("Load Generator cannot be created from type: ${config.loadGeneratorConfig.loadGeneratorType}. Possible values are: constant, bound or increasing")
            }

    }
}