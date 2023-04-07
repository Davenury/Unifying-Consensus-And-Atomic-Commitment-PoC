package com.github.davenury.test

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.Changes
import com.github.davenury.tests.strategies.changes.DefaultChangeStrategy
import com.github.davenury.tests.strategies.peersets.GetPeersStrategy
import com.github.davenury.tests.strategies.peersets.RandomPeersWithDelayOnConflictStrategy
import io.mockk.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock

class ChangesTest {

    companion object {
        private val peers: Map<PeersetId, List<PeerAddress>> = mapOf(
            PeersetId("peerset0") to listOf(
                PeerAddress(PeerId("peer0"), "localhost0:8080"),
                PeerAddress(PeerId("peer1"), "localhost0:8081"),
                PeerAddress(PeerId("peer2"), "localhost0:8082"),
            ),
            PeersetId("peerset1") to listOf(
                PeerAddress(PeerId("peer3"), "localhost1:8080"),
            ),
            PeersetId("peerset2") to listOf(
                PeerAddress(PeerId("peer4"), "localhost2:8080"),
            ),
            PeersetId("peerset3") to listOf(
                PeerAddress(PeerId("peer5"), "localhost3:8080"),
                PeerAddress(PeerId("peer6"), "localhost3:8081"),
                PeerAddress(PeerId("peer7"), "localhost3:8082"),
            ),
            PeersetId("peerset4") to listOf(
                PeerAddress(PeerId("peer8"), "localhost4:8080"),
            ),
            PeersetId("peerset5") to listOf(
                PeerAddress(PeerId("peer9"), "localhost5:8080"),
            ),
        )

        private const val ownAddress = "http://localhost:8080"
    }

    @Test
    fun `should call lock, when list of available peersets is empty`(): Unit = runBlocking {
        // given - changes
        val sender = DummySender(peersets = peers, shouldNotify = false)
        val lockMock = mockk<Lock>()
        val conditionMock = mockk<Condition>()
        val subject = Changes(
            peers,
            sender,
            RandomPeersWithDelayOnConflictStrategy(peers.keys.toList(), lockMock, conditionMock),
            DefaultChangeStrategy(ownAddress),
            null
        )
        sender.setChanges(subject)
        val phaser = Phaser(peers.keys.size)
        phaser.register()
        val handleNotificationCounter = AtomicInteger(0)

        every { conditionMock.await() } answers {
            if (handleNotificationCounter.getAndIncrement() < 1) {
                launch {
                    subject.handleNotification(
                        Notification(
                            sender.getLastChange(),
                            ChangeResult(ChangeResult.Status.SUCCESS),
                            PeerAddress(PeerId("peer0"), "peer0-peerset0-service")
                        )
                    )
                }
            }
        }
        every { conditionMock.signalAll() } just Runs
        every { lockMock.lock() } just Runs
        every { lockMock.unlock() } just Runs

        // when - executing changes without notyfing about ending
        // after this all peersets are occupied with a change
        peers.keys.forEach { _ ->
            withContext(Dispatchers.IO) {
                launch {
                    subject.introduceChange(1)
                    phaser.arrive()
                }
            }
        }

        phaser.arriveAndAwaitAdvance()

        // and - another change
        val phase2Phaser = Phaser(2)
        withContext(Dispatchers.IO) {
            launch {
                subject.introduceChange(1)
                phase2Phaser.arrive()
            }
        }

        phase2Phaser.arriveAndAwaitAdvance()

        verify(atLeast = 1) { conditionMock.await() }

        // we handle notification in response to await, in this function there's a condition.signalAll()
        verify(exactly = 1) { conditionMock.signalAll() }
    }

    @Test
    fun `should be able to unlock peersets`(): Unit = runBlocking {
        val sender = DummySender(shouldNotify = false, peers)
        val subject = Changes(
            peers,
            sender,
            RandomPeersWithDelayOnConflictStrategy(peers.keys.toList()),
            DefaultChangeStrategy(ownAddress),
            null
        )
        sender.setChanges(subject)

        val phaser = Phaser(2)

        // when - executing changes without notyfing about ending
        // after this all peersets are occupied with a change
        peers.keys.forEach { key ->
            withContext(Dispatchers.IO) {
                if (key == PeersetId("peerset0")) {
                    subject.introduceChange(1)
                    phaser.arrive()
                } else {
                    subject.introduceChange(1)
                }
            }
        }

        phaser.arriveAndAwaitAdvance()

        // and - we explicitly unlock one peer
        sender.notify(sender.getLastChange())

        // then - should throw exception when adding another change
        expectCatching {
            subject.introduceChange(1)
        }.isSuccess()
    }

    @Test
    fun `should be able to execute changes without any errors and with valid parents ids`(): Unit = runBlocking {
        val changesToExecute = 10
        val phaser = Phaser(changesToExecute)
        phaser.register()
        val sender = DummySender(shouldNotify = true, phaser = phaser, peersets = peers)
        val subject = Changes(
            peers, sender, RandomPeersWithDelayOnConflictStrategy(peers.keys.toList()), DefaultChangeStrategy(
                ownAddress
            ),
            null
        )
        sender.setChanges(subject)

        // max 3 threads as we have 6 possible peersets and we cannot execute change to all of them plus 1
        val ctx = Executors.newFixedThreadPool(3).asCoroutineDispatcher()

        for (i in (1..changesToExecute)) {
            launch(ctx) {
                subject.introduceChange(2)
            }
        }

        phaser.arriveAndAwaitAdvance()

        val changes = sender.appearedChanges
        expectThat(areChangesValid(changes)).isEqualTo(true)
    }

    @Test
    fun `handle notification should be idempotent`() {
        val peersets = (1..10).map { PeersetId("peerset$it") }
        val counter = AtomicInteger(0)
        val strategy = object : GetPeersStrategy {
            override suspend fun getPeersets(numberOfPeersets: Int, changeId: String): List<PeersetId> =
                peersets.shuffled().take(numberOfPeersets)

            override suspend fun freePeersets(peersetsId: List<PeersetId>, changeId: String) {}

            override suspend fun handleNotification(notification: Notification) {
                counter.incrementAndGet()
            }
        }

        val changes = Changes(
            peers, DummySender(shouldNotify = false, peers), strategy, DefaultChangeStrategy(
                ownAddress
            ),
            null
        )

        val singleChange = AddUserChange(
            "userName",
            peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), "a2fasda2f"))
        )
        repeat(3) {
            runBlocking {
                changes.handleNotification(
                    Notification(
                        singleChange,
                        ChangeResult(ChangeResult.Status.SUCCESS),
                        PeerAddress(PeerId("peer0"), "peer0-peerset0-service")
                    )
                )
            }
        }

        expectThat(counter.get()).isEqualTo(1)
    }

    private fun areChangesValid(changes: List<Pair<PeerAddress, Change>>): Boolean {
        val mapOfChanges = (0..changes.size)
            .map { PeersetId("peerset$it") }
            .associateWith { InitialHistoryEntry.getId() }
            .toMutableMap()
        changes.forEach { (_, change) ->
            change.peersets.forEach { (peersetId, parentId) ->
                if (parentId != mapOfChanges[peersetId]) {
                    println("ParentId differs for change in peerset $peersetId - parentId: ${parentId}, expected parent id: ${mapOfChanges[peersetId]}")
                    return false
                }

                mapOfChanges[peersetId] = change.toHistoryEntry(peersetId).getId()
            }
        }
        return true
    }
}
