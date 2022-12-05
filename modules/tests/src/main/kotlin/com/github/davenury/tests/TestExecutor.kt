package com.github.davenury.tests

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.random.Random

class TestExecutor(
    private val numberOfRequestsToSendToSinglePeerset: Int,
    private val numberOfRequestsToSendToMultiplePeersets: Int,
    private val timeOfSimulation: Duration,
    // determines how many peersets can be included in one change
    private val maxPeersetsInChange: Int,
    private val changes: Changes
) {
    private val overallNumberOfRequests =
        numberOfRequestsToSendToMultiplePeersets + numberOfRequestsToSendToSinglePeerset
    private val sendRequestBreak = timeOfSimulation.dividedBy(overallNumberOfRequests.toLong())
    private var sentSinglePeersetChanges = 0
    private var sentMulitplePeersetChanges = 0

    private val channel: ReceiveChannel<Unit> = ticker(sendRequestBreak.toMillis(), 0)

    suspend fun startTest() {
        logger.info("Test starts. Number of singlePeerset requests: $numberOfRequestsToSendToSinglePeerset, number of multiplePeersets requests: $numberOfRequestsToSendToMultiplePeersets\n" +
                "time of simulation: ${timeOfSimulation.toSeconds()}s. Requests are sent in ${sendRequestBreak.toMillis()}ms breaks.")
        withContext(ctx) {
            for (i in (1..overallNumberOfRequests)) {
                channel.receive()
                launch {
                    logger.info("Introducing change $i")
                    changes.introduceChange(determineNumberOfPeersets())
                }
            }
            channel.cancel()
        }
        logger.info("Test Executor sent ended it's work.")
    }

    private fun determineNumberOfPeersets(): Int {

        if (sentSinglePeersetChanges < numberOfRequestsToSendToSinglePeerset && sentMulitplePeersetChanges < numberOfRequestsToSendToMultiplePeersets) {
            return (Random.nextInt(maxPeersetsInChange) + 1).also {
                if (it == 1) {
                    sentSinglePeersetChanges++
                } else {
                    sentMulitplePeersetChanges++
                }
            }
        }

        if (sentSinglePeersetChanges < numberOfRequestsToSendToSinglePeerset) {
            sentSinglePeersetChanges++
            return 1
        }

        sentMulitplePeersetChanges++
        return Random.nextInt(maxPeersetsInChange - 1) + 2
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestExecutor")

        private val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()
    }
}
