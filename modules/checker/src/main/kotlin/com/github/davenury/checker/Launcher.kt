package com.github.davenury.checker

import com.github.davenury.common.PeersetId
import com.github.davenury.common.loadConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.system.exitProcess

fun main() {
    val config = loadConfig<Config>()
    try {
        Launcher(config).launch()
        exitProcess(0)
    } catch (e: Exception) {
        exitProcess(1)
    }
}

class Launcher(
    private val config: Config,
    private val changesGetter: ChangesGetter = HttpChangesGetter()
) {

    fun launch() {
        val results: List<Future<CheckResult>> = context.invokeAll(
            config.getPeersets().map { (peersetId, peers) ->
                Callable {
                    SinglePeersetChecker(peersetId, peers, changesGetter).checkPeerset()
                }
            }
        )

        logger.info("Got results from all peersets")
        assertChangesBetweenPeersets(results.map { it.get() })
    }

    private fun assertChangesBetweenPeersets(results: List<CheckResult>) {
        if (results.any { !it.doesPeersetHaveTheSameChanges }) {
            logger.error("Changes aren't the same in peersets: ${results.filter { it.doesPeersetHaveTheSameChanges }.map { it.peersetId.peersetId }}")
            throw ChangesArentTheSameException(ChangesArentTheSameReason.DIFFERENT_CHANGES)
        }

        val changeToPeersetInformation = mutableMapOf<String, PeersetInformation>()

        results.forEach { result ->
            result.multiplePeersetChanges.entries.forEach { (changeId, multiplePeersetChange) ->
                changeToPeersetInformation[changeId] = changeToPeersetInformation.getOrDefault(
                    changeId,
                    PeersetInformation(multiplePeersetChange.expectedPeersets, multiplePeersetChange.observedPeersets.toMutableSet())
                ).apply {
                    this.observedPeersets.addAll(multiplePeersetChange.observedPeersets)
                }
            }
        }

        changeToPeersetInformation.forEach { (changeId, info) ->
            if (info.expectedPeersets.toSet() != info.observedPeersets) {
                logger.error("Change: $changeId was observed in ${info.observedPeersets} but is expected to be in ${info.expectedPeersets}")
                throw ChangesArentTheSameException(ChangesArentTheSameReason.DIFFERENT_CHANGES_BETWEEN_PEERSETS)
            }
        }

        logger.info("Everything's fine")
    }

    private data class PeersetInformation(
        val expectedPeersets: List<PeersetId>,
        val observedPeersets: MutableSet<PeersetId> = mutableSetOf(),
    )

    private val context = Executors.newFixedThreadPool(config.numberOfThreads)

    companion object {
        private val logger = LoggerFactory.getLogger("Launcher")
    }
}

