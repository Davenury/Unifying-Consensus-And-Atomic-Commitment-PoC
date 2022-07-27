package github.davenury.ucac.domain

import github.davenury.ucac.common.*
import github.davenury.ucac.consensus.raft.infrastructure.DummyConsensusProtocol
import github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import github.davenury.ucac.getOtherPeers
import github.davenury.ucac.gpac.domain.Accept
import github.davenury.ucac.gpac.domain.GPACProtocolImpl
import github.davenury.ucac.gpac.domain.ProtocolClientImpl
import github.davenury.ucac.gpac.domain.TransactionBlockerImpl
import github.davenury.ucac.utils.PeerThree
import github.davenury.ucac.utils.PeerTwo
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isEqualTo
import java.time.Duration
import java.util.concurrent.Executors

class LeaderTest {

    val ctx = Executors.newCachedThreadPool().asCoroutineDispatcher()

    @BeforeEach
    fun setup() {
        subject =
            GPACProtocolImpl(
                historyManagement,
                3,
                timer,
                client,
                transactionBlocker,
                otherPeers,
                me = 8080,
                myPeersetId = 0
            )
    }

    @Test
    fun `should load only other peers`() {
        val nodeId = 2
        val peersetId = 1

        val allPeers = listOf(listOf("peer1:8080", "peer2:8080", "peer3:8080"))
        expectThat(getOtherPeers(allPeers, nodeId, peersetId).flatten())
            .containsExactlyInAnyOrder("peer1:8080", "peer3:8080")
    }

    @Test
    fun `should load only other peers - mutliple peersets`() {
        val nodeId = 2
        val peersetId = 1

        val allPeers =
            listOf(
                listOf("peer1:8080", "peer2:8080", "peer3:8080"),
                listOf("peer4:8080", "peer5:8080", "peer6:8080")
            )
        expectThat(getOtherPeers(allPeers, nodeId, peersetId).flatten())
            .containsExactlyInAnyOrder(
                "peer1:8080",
                "peer3:8080",
                "peer4:8080",
                "peer5:8080",
                "peer6:8080"
            )
    }

    @Test
    fun `should load only other peers - localhost version`() {
        val nodeId = 2
        val peersetId = 1

        expectThat(getOtherPeers(allPeers, nodeId, peersetId, 9090).flatten())
            .containsExactlyInAnyOrder("localhost:9091", "localhost:9093")
    }

    @Test
    fun `should throw max retires exceeded, when too many times tried to be a leader`() {
        PeerTwo.stubForNotElectingYou()
        PeerThree.stubForNotElectingYou()

        expectThrows<MaxTriesExceededException> { subject.performProtocolAsLeader(changeDto) }

        // assert that we're actually asking 3 times
        PeerTwo.verifyMaxRetriesForElectionPassed(3)
        PeerThree.verifyMaxRetriesForElectionPassed(3)
    }

    @Test
    fun `should throw TooFewResponsesException when not enough responses for agree message`() {
        PeerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerTwo.stubForNotAgree()
        PeerThree.stubForNotAgree()

        expectThrows<TooFewResponsesException> { subject.performProtocolAsLeader(changeDto) }

        PeerTwo.verifyAgreeStub(1)
        PeerThree.verifyAgreeStub(1)
    }

    @Test
    fun `should perform operation to the end`(): Unit = runBlocking {
        PeerTwo.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerThree.stubForElectMe(10, Accept.COMMIT, 10, null, false)
        PeerTwo.stubForAgree(10, Accept.COMMIT)
        PeerThree.stubForAgree(10, Accept.COMMIT)
        PeerTwo.stubForApply()
        PeerThree.stubForApply()

        runBlocking { subject.performProtocolAsLeader(changeDto) }

        expectThat(historyManagement.getLastChange())
            .isEqualTo(ChangeWithAcceptNum(changeDto.toChange(), 1))
    }

    private val allPeers = listOf(listOf("localhost:9091", "localhost:9092", "localhost:9093"))
    private val otherPeers = getOtherPeers(allPeers, 1, 1, 9090)
    private val consensusProtocol = DummyConsensusProtocol()
    private val historyManagement = InMemoryHistoryManagement(consensusProtocol)
    private val timer = ProtocolTimerImpl(Duration.ofSeconds(1), Duration.ofSeconds(1), ctx)
    private val client = ProtocolClientImpl()
    private val transactionBlocker = TransactionBlockerImpl()
    private var subject =
        GPACProtocolImpl(
            historyManagement,
            3,
            timer,
            client,
            transactionBlocker,
            otherPeers,
            me = 8080,
            myPeersetId = 0
        )
    private val changeDto = ChangeDto(mapOf("operation" to "ADD_USER", "userName" to "userName"))
}
