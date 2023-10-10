package com.github.davenury.checker

import com.github.davenury.common.*
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.*

class SinglePeersetCheckerSpec {

    @Test
    fun `should return expected result when there's only one peer in peerset`() {
        val changes = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null)))
            )
        )

        val peers = listOf(PeerAddress(PeerId("p1"), "localhost:0"))
        val subject = SinglePeersetChecker(
            PeersetId("p1"),
            peers = peers,
            DummyChangesGetter(peers.associateWith { changes })
        )

        val result = subject.checkPeerset()

        expectThat(result.doesPeersetHaveTheSameChanges).isTrue()
        expectThat(result.reason).isNull()
        expectThat(result.multiplePeersetChanges).isEmpty()
    }

    @Test
    fun `should return expected result, when there are more peers in peerset but no change is multiple-peerset`() {
        val changes = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null)))
            )
        )

        val peers = listOf(
            PeerAddress(PeerId("p1"), "localhost:0"),
            PeerAddress(PeerId("p2"), "localhost:0"),
            PeerAddress(PeerId("p3"), "localhost:0"),
        )
        val subject = SinglePeersetChecker(
            PeersetId("p1"),
            peers = peers,
            DummyChangesGetter(peers.associateWith { changes })
        )

        val result = subject.checkPeerset()

        expectThat(result.doesPeersetHaveTheSameChanges).isTrue()
        expectThat(result.reason).isNull()
        expectThat(result.multiplePeersetChanges).isEmpty()
    }

    @Test
    fun `should return expected result when changes are not compatible within peerset (different sizes)`() {
        val changes = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null)))
            )
        )

        val changes2 = Changes(
            changes + listOf(
                AddUserChange(
                    "user2",
                    peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null))
                )
            )
        )

        val peers = listOf(
            PeerAddress(PeerId("p1"), "localhost:0"),
            PeerAddress(PeerId("p2"), "localhost:0"),
            PeerAddress(PeerId("p3"), "localhost:0"),
        )
        val subject = SinglePeersetChecker(
            PeersetId("p1"),
            peers = peers,
            DummyChangesGetter(peers.associateWith { changes } + mapOf(peers[0] to changes2))
        )

        val result = subject.checkPeerset()

        expectThat(result.doesPeersetHaveTheSameChanges).isFalse()
        expectThat(result.reason).isEqualTo(ChangesArentTheSameReason.DIFFERENT_SIZES)
        expectThat(result.multiplePeersetChanges).isEmpty()
    }

    @Test
    fun `should return expected result when changes are not compatible within peerset (different changes)`() {
        val changes = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null)))
            )
        )

        val changes2 = Changes(
            listOf(
                AddUserChange("user2", peersets = listOf(ChangePeersetInfo(PeersetId("p1"), null)))
            )
        )

        val peers = listOf(
            PeerAddress(PeerId("p1"), "localhost:0"),
            PeerAddress(PeerId("p2"), "localhost:0"),
            PeerAddress(PeerId("p3"), "localhost:0"),
        )
        val subject = SinglePeersetChecker(
            PeersetId("p1"),
            peers = peers,
            DummyChangesGetter(peers.associateWith { changes } + mapOf(peers[0] to changes2))
        )

        val result = subject.checkPeerset()

        expectThat(result.doesPeersetHaveTheSameChanges).isFalse()
        expectThat(result.reason).isEqualTo(ChangesArentTheSameReason.DIFFERENT_CHANGES)
        expectThat(result.multiplePeersetChanges).isEmpty()
    }

    @Test
    fun `should return expected result for changes that are multiple-peerset`() {
        val changes = Changes(
            listOf(
                AddUserChange(
                    "user1", peersets = listOf(
                        ChangePeersetInfo(PeersetId("p1"), null), ChangePeersetInfo(
                            PeersetId("p2"), null
                        )
                    )
                )
            )
        )

        val peers = listOf(PeerAddress(PeerId("p1"), "localhost:0"))
        val subject = SinglePeersetChecker(
            PeersetId("p1"),
            peers = peers,
            DummyChangesGetter(peers.associateWith { changes })
        )

        val result = subject.checkPeerset()

        expectThat(result.doesPeersetHaveTheSameChanges).isTrue()
        expectThat(result.reason).isNull()
        expectThat(result.multiplePeersetChanges.size).isEqualTo(1)
        expectThat(result.multiplePeersetChanges[changes[0].id]).isEqualTo(MultiplePeersetChange(
            expectedPeersets = listOf(PeersetId("p1"), PeersetId("p2")),
            observedPeersets = listOf(PeersetId("p1"))
        ))
    }

}