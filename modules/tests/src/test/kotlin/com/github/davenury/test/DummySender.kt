package com.github.davenury.test

import com.github.davenury.common.*
import com.github.davenury.tests.ChangeState
import com.github.davenury.tests.Changes
import com.github.davenury.tests.Sender
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.Phaser

class DummySender(
    private val shouldNotify: Boolean,
    private val peersets: Map<PeersetId, List<PeerAddress>>,
    private val phaser: Phaser? = null
) : Sender {

    private lateinit var changes: Changes

    val appearedChanges = mutableListOf<Pair<PeerAddress, Change>>()
    val mutex = Mutex()

    override suspend fun executeChange(address: PeerAddress, change: Change, peersetId: PeersetId): ChangeState {
        mutex.withLock {
            appearedChanges.add(Pair(address, change))
        }
        if (shouldNotify) {
            notify(change)
        }
        phaser?.arrive()
        return ChangeState.ACCEPTED
    }

    suspend fun notify(change: Change) {
        changes.handleNotification(
            Notification(
                change = change,
                result = ChangeResult(ChangeResult.Status.SUCCESS),
                sender = PeerAddress(PeerId("peer0"), "peer0-peerset0-service")
            )
        )
    }

    override suspend fun getConsensusLeaderId(address: PeerAddress, peersetId: PeersetId): PeerId? {
        return peersets.values.find { address in it }?.first()?.peerId
    }

    fun setChanges(changes: Changes) {
        this.changes = changes
    }

    fun getLastChange(): Change = appearedChanges.last().second
}
