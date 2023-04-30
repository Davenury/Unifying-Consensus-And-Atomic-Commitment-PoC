package com.github.davenury.ucac.twoPC

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.TwoPCConfig
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.commitment.twopc.TwoPCProtocolClient
import com.github.davenury.ucac.commitment.twopc.TwoPCRequestResponse
import com.github.davenury.ucac.common.PeerResolver
import io.mockk.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.time.Duration
import java.util.concurrent.Executors

class GetProposePhaseResponsesTest {

    private val protocolClientMock = mockk<TwoPCProtocolClient>()
    private val peerResolver = PeerResolver(
        currentPeer = PeerId("peer0"),
        peers = mapOf(
            PeerId("peer0") to PeerAddress(PeerId("peer0"), "localhost:0"),
            PeerId("peer1") to PeerAddress(PeerId("peer1"), "localhost:0"),
            PeerId("peer2") to PeerAddress(PeerId("peer2"), "localhost:0"),
            PeerId("peer3") to PeerAddress(PeerId("peer3"), "localhost:0"),
            PeerId("peer4") to PeerAddress(PeerId("peer4"), "localhost:0"),
        ),
        peersets = mapOf(
            PeersetId("peerset0") to listOf(PeerId("peer0"), PeerId("peer1")),
            PeersetId("peerset1") to listOf(PeerId("peer2"), PeerId("peer3")),
            PeersetId("peerset2") to listOf(PeerId("peer4")),
        )
    )
    private val subject = TwoPC(
        peersetId = PeersetId("peerset0"),
        history = mockk(),
        twoPCConfig = TwoPCConfig(),
        ctx = Executors.newSingleThreadExecutor().asCoroutineDispatcher(),
        protocolClient = protocolClientMock,
        consensusProtocol = mockk(),
        peerResolver = peerResolver,
        signalPublisher = mockk(),
        isMetricTest = false,
        changeNotifier = mockk(),
    )

    @Test
    fun `should be able to work correctly with no redirects`(): Unit = runBlocking {

        coEvery { protocolClientMock.sendAccept(any(), any()) } returns mapOf(
            peerResolver.resolve(PeerId("peer3")) to TwoPCRequestResponse(true, peersetId = PeersetId("peerset1"))
        )

        val decision = subject.getProposePhaseResponses(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer3"))),
            change = AddUserChange(userName = ""),
            mapOf()
        )

        expectThat(decision).isTrue()
        // should not populate the map if no redirects were thrown
        expectThat(subject.currentConsensusLeaders).isEmpty()
    }

    @Test
    fun `should repeat the accept only to peersets that returned redirects`(): Unit = runBlocking {
        coEvery { protocolClientMock.sendAccept(any(), any()) } returnsMany listOf(
            // first return
            mapOf(
                peerResolver.resolve(PeerId("peer3")) to TwoPCRequestResponse(success = false, redirect = true, newConsensusLeaderId = PeerId("peer2"), newConsensusLeaderPeersetId = PeersetId("peerset1"), peersetId = PeersetId("peerset1")),
                peerResolver.resolve(PeerId("peer4")) to TwoPCRequestResponse(success = true, peersetId = PeersetId("peerset2"))
            ),
            // second return
            mapOf(
                peerResolver.resolve(PeerId("peer2")) to TwoPCRequestResponse(success = true, peersetId = PeersetId("peerset1"))
            )
        )

        val decision = subject.getProposePhaseResponses(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer3")), PeersetId("peerset2") to peerResolver.resolve(PeerId("peer4"))),
            change = AddUserChange(""),
            mapOf()
        )

        expectThat(decision).isTrue()

        // should remember last consensus leader
        expectThat(subject.currentConsensusLeaders[PeersetId("peerset1")]).isEqualTo(peerResolver.resolve(PeerId("peer2")))

        // and the calls should be valid
        coVerify(exactly = 1) { protocolClientMock.sendAccept(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer3")), PeersetId("peerset2") to  peerResolver.resolve(PeerId("peer4"))),
            any()
        ) }
        coVerify(exactly = 1) { protocolClientMock.sendAccept(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer2"))),
            any()
        ) }
    }

    @Test
    fun `should respect decision false of recent responses`(): Unit = runBlocking {
        coEvery { protocolClientMock.sendAccept(any(), any()) } returnsMany listOf(
            // first return
            mapOf(
                peerResolver.resolve(PeerId("peer3")) to TwoPCRequestResponse(success = false, redirect = true, newConsensusLeaderId = PeerId("peer2"), newConsensusLeaderPeersetId = PeersetId("peerset1"), peersetId = PeersetId("peerset1")),
                peerResolver.resolve(PeerId("peer4")) to TwoPCRequestResponse(success = false, peersetId = PeersetId("peerset2"))
            ),
            // second return
            mapOf(
                peerResolver.resolve(PeerId("peer2")) to TwoPCRequestResponse(success = true, peersetId = PeersetId("peerset1"))
            )
        )

        val decision = subject.getProposePhaseResponses(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer3")), PeersetId("peerset2") to  peerResolver.resolve(PeerId("peer4"))),
            change = AddUserChange(""),
            mapOf()
        )

        expectThat(decision).isFalse()

        // should remember last consensus leader
        expectThat(subject.currentConsensusLeaders[PeersetId("peerset1")]).isEqualTo(peerResolver.resolve(PeerId("peer2")))

        // and the calls should be valid
        coVerify(exactly = 1) { protocolClientMock.sendAccept(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer3")), PeersetId("peerset2") to  peerResolver.resolve(PeerId("peer4"))),
            any()
        ) }
        coVerify(exactly = 1) { protocolClientMock.sendAccept(
            mapOf(PeersetId("peerset1") to peerResolver.resolve(PeerId("peer2"))),
            any()
        ) }
    }

}