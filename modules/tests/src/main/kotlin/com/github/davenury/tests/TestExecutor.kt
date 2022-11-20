package com.github.davenury.tests

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import java.time.Duration
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
        withContext(Dispatchers.IO) {
            for (i in (1..overallNumberOfRequests)) {
                channel.receive()
                launch {
                    changes.introduceChange(determineNumberOfPeersets())
                }
            }
            channel.cancel()
        }
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

        return Random.nextInt(maxPeersetsInChange - 1) + 2
    }
}
