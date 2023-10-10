package com.github.davenury.checker

import com.github.davenury.common.*
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

class SinglePeersetChecker(
    private val peersetId: PeersetId,
    private val peers: List<PeerAddress>,
    private val changesGetter: ChangesGetter,
) {

    fun checkPeerset(): CheckResult {
        val changes = getPeersetChanges()

        val compatibilityResult = areChangesEqual(changes.values.toList())

        if (!compatibilityResult.ok) {
            return CheckResult(peersetId, false, reason = compatibilityResult.reason)
        }

        val multiplePeersetChanges = getMultiplePeersetChanges(changes.values.toList()[0])

        return CheckResult(peersetId, true, multiplePeersetChanges = multiplePeersetChanges)
    }

    private fun getPeersetChanges(): Map<PeerId, Changes> {
        logger.info("Getting changes from peerset: ${peersetId.peersetId}")
        return runBlocking {
            peers.associate {
                it.peerId to async {
                    changesGetter.getChanges(it, peersetId)
                }.await()
            }
        }
    }

    private fun areChangesEqual(lists: List<Changes>): ChangesEqualityResult {
        val distinctLists = lists.distinct()
        if (distinctLists.size == 1) {
            return ChangesEqualityResult(true)
        }

        // check for different sizes
        distinctLists.map { it.size }.reduce { first, second ->
            if (first == second) first else return ChangesEqualityResult(
                false,
                ChangesArentTheSameReason.DIFFERENT_SIZES
            )
        }

        return ChangesEqualityResult(false, ChangesArentTheSameReason.DIFFERENT_CHANGES)
    }

    private fun getMultiplePeersetChanges(changes: Changes): Map<String, MultiplePeersetChange> =
        changes
            .filter { it.peersets.size > 1 }
            // TwoPC Changes does not need to be in all peersets
            .filterNot { it is TwoPCChange }
            .map { change ->
                change.id to MultiplePeersetChange(
                    change.peersets.map { it.peersetId },
                    listOf(peersetId)
                )
            }
            .toMap()


    companion object {
        private val logger = LoggerFactory.getLogger("SinglePeersetChecker")
    }
}

data class ChangesEqualityResult(
    val ok: Boolean,
    val reason: ChangesArentTheSameReason? = null,
)

data class CheckResult(
    val peersetId: PeersetId,
    val doesPeersetHaveTheSameChanges: Boolean,
    val reason: ChangesArentTheSameReason? = null,
    val multiplePeersetChanges: Map<String, MultiplePeersetChange> = mapOf(),
)

data class MultiplePeersetChange(
    val expectedPeersets: List<PeersetId>,
    val observedPeersets: List<PeersetId>
)