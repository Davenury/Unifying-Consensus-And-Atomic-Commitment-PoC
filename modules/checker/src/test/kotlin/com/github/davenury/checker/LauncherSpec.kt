package com.github.davenury.checker

import com.github.davenury.common.*
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isA
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import java.util.*

class LauncherSpec {

    @Test
    fun `should be able to check single peer single peerset`() {
        val config = Config(
            peers = "peer0=localhost0",
            peersets = "peerset0=peer0"
        )

        val changes = Changes(
            listOf(AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))))
        )

        val subject = Launcher(config, DummyChangesGetter(mapOf(config.getPeer(PeerId("peer0"))!! to changes)))

        expectCatching {
            subject.launch()
        }.isSuccess()
    }

    @Test
    fun `should be able to check multiple peers single peerset`() {
        val config = Config(
            peers = "peer0=localhost0;peer1=localhost0",
            peersets = "peerset0=peer0,peer1"
        )

        val changes = Changes(
            listOf(AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))))
        )

        val subject = Launcher(
            config, DummyChangesGetter(
                mapOf(
                    config.getPeer(PeerId("peer0"))!! to changes,
                    config.getPeer(PeerId("peer1"))!! to changes,
                )
            )
        )

        expectCatching {
            subject.launch()
        }.isSuccess()
    }

    @Test
    fun `should be able to check multiple peers multiple peersets when their changes are separate`() {
        val config = Config(
            peers = "peer0=localhost0;peer1=localhost0;peer2=localhost:0;peer3=localhost:0",
            peersets = "peerset0=peer0,peer1;peerset1=peer2,peer3"
        )

        val zeroChanges = Changes(
            listOf(AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))))
        )

        val firstChanges = Changes(
            listOf(AddUserChange("user2", peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), null))))
        )

        val subject = Launcher(
            config, DummyChangesGetter(
                mapOf(
                    config.getPeer(PeerId("peer0"))!! to zeroChanges,
                    config.getPeer(PeerId("peer1"))!! to zeroChanges,
                    config.getPeer(PeerId("peer2"))!! to firstChanges,
                    config.getPeer(PeerId("peer3"))!! to firstChanges,
                )
            )
        )

        expectCatching {
            subject.launch()
        }.isSuccess()
    }

    @Test
    fun `should be able to check multiple peers multiple peersets when their changes are not separate`() {
        val config = Config(
            peers = "peer0=localhost0;peer1=localhost0;peer2=localhost:0;peer3=localhost:0",
            peersets = "peerset0=peer0,peer1;peerset1=peer2,peer3"
        )

        val commonChangeId = UUID.randomUUID()

        val zeroChanges = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))),
                AddUserChange(
                    "user3",
                    peersets = listOf(
                        ChangePeersetInfo(PeersetId("peerset0"), null),
                        ChangePeersetInfo(PeersetId("peerset1"), null)
                    ),
                    id = commonChangeId.toString()
                )
            ),
        )

        val firstChanges = Changes(
            listOf(
                AddUserChange("user2", peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), null))),
                AddUserChange(
                    "user3",
                    peersets = listOf(
                        ChangePeersetInfo(PeersetId("peerset0"), null),
                        ChangePeersetInfo(PeersetId("peerset1"), null)
                    ),
                    id = commonChangeId.toString()
                )
            )
        )

        val subject = Launcher(
            config, DummyChangesGetter(
                mapOf(
                    config.getPeer(PeerId("peer0"))!! to zeroChanges,
                    config.getPeer(PeerId("peer1"))!! to zeroChanges,
                    config.getPeer(PeerId("peer2"))!! to firstChanges,
                    config.getPeer(PeerId("peer3"))!! to firstChanges,
                )
            )
        )

        expectCatching {
            subject.launch()
        }.isSuccess()
    }

    @Test
    fun `should forgive lack of TwoPC changes in one peerset`() {
        val config = Config(
            peers = "peer0=localhost0;peer1=localhost0;peer2=localhost:0;peer3=localhost:0",
            peersets = "peerset0=peer0,peer1;peerset1=peer2,peer3"
        )

        val addUser3Change = AddUserChange("user3", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null), ChangePeersetInfo(PeersetId("peerset1"), null)))
        val zeroChanges = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))),
                TwoPCChange(twoPCStatus = TwoPCStatus.ACCEPTED, change = addUser3Change, peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null), ChangePeersetInfo(PeersetId("peerset1"), null))),
                TwoPCChange(twoPCStatus = TwoPCStatus.ABORTED, change = addUser3Change, peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null), ChangePeersetInfo(PeersetId("peerset1"), null)))
            ),
        )

        val firstChanges = Changes(
            listOf(
                AddUserChange("user2", peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), null))),
            )
        )

        val subject = Launcher(
            config, DummyChangesGetter(
                mapOf(
                    config.getPeer(PeerId("peer0"))!! to zeroChanges,
                    config.getPeer(PeerId("peer1"))!! to zeroChanges,
                    config.getPeer(PeerId("peer2"))!! to firstChanges,
                    config.getPeer(PeerId("peer3"))!! to firstChanges,
                )
            )
        )

        expectCatching {
            subject.launch()
        }.isSuccess()
    }

    @Test
    fun `should be able to check multiple peers multiple peersets when one peerset did not received a change`() {
        val config = Config(
            peers = "peer0=localhost0;peer1=localhost0;peer2=localhost:0;peer3=localhost:0",
            peersets = "peerset0=peer0,peer1;peerset1=peer2,peer3"
        )

        val commonChangeId = UUID.randomUUID()

        val zeroChanges = Changes(
            listOf(
                AddUserChange("user1", peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), null))),
                AddUserChange(
                    "user3",
                    peersets = listOf(
                        ChangePeersetInfo(PeersetId("peerset0"), null),
                        ChangePeersetInfo(PeersetId("peerset1"), null)
                    ),
                    id = commonChangeId.toString()
                )
            ),
        )

        val firstChanges = Changes(
            listOf(
                AddUserChange("user2", peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), null))),
            )
        )

        val subject = Launcher(
            config, DummyChangesGetter(
                mapOf(
                    config.getPeer(PeerId("peer0"))!! to zeroChanges,
                    config.getPeer(PeerId("peer1"))!! to zeroChanges,
                    config.getPeer(PeerId("peer2"))!! to firstChanges,
                    config.getPeer(PeerId("peer3"))!! to firstChanges,
                )
            )
        )

        expectCatching {
            subject.launch()
        }.isFailure()
            .isA<ChangesArentTheSameException>()
    }

}