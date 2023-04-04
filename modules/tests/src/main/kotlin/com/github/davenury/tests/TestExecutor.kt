package com.github.davenury.tests

import com.github.davenury.tests.strategies.load.LoadGenerator
import org.slf4j.LoggerFactory
import kotlin.random.Random

class TestExecutor(
    private val changes: Changes,
    private val config: Config,
) {

    private var sentSinglePeersetChanges = 0
    private var sentMulitplePeersetChanges = 0

    private val loadGenerator: LoadGenerator = LoadGenerator.createFromConfig(config)

    suspend fun startTest() {
        logger.info("Test starts.")
        loadGenerator.generate()
        loadGenerator.subscribe { changes.introduceChange(determineNumberOfPeersets()) }
        logger.info("Test Executor sent ended it's work.")
    }

    private fun determineNumberOfPeersets(): Int {

        if (config.fixedPeersetsInChange != null) {
            return config.fixedPeersetsInChange.toInt().also {
                if (it == 1) {
                    sentSinglePeersetChanges++
                } else {
                    sentMulitplePeersetChanges++
                }
            }
        }

        if (sentSinglePeersetChanges < config.numberOfRequestsToSendToSinglePeerset!! && sentMulitplePeersetChanges < config.numberOfRequestsToSendToMultiplePeersets!!) {
            return (Random.nextInt(config.maxPeersetsInChange) + 1).also {
                if (it == 1) {
                    sentSinglePeersetChanges++
                } else {
                    sentMulitplePeersetChanges++
                }
            }
        }

        if (sentSinglePeersetChanges < config.numberOfRequestsToSendToSinglePeerset) {
            sentSinglePeersetChanges++
            return 1
        }

        sentMulitplePeersetChanges++
        return Random.nextInt(config.maxPeersetsInChange - 1) + 2
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestExecutor")
    }
}
