package com.github.davenury.tests.strategies.changes

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.PeersetId
import com.github.davenury.tests.OnePeersetChanges
import java.util.concurrent.atomic.AtomicInteger

class OnlyProcessableConflictsChangeStrategy(
    private val ownAddress: String
): CreateChangeStrategy {

    private var counter = AtomicInteger(0)

    override fun createChange(ids: List<PeersetId>, changes: Map<PeersetId, OnePeersetChanges>): Change {

        val peersets = List(ids.size) {
            if (it == 0) {
                ChangePeersetInfo(ids[it], changes[ids[it]]!!.getCurrentParentId())
            } else {
                ChangePeersetInfo(ids[it], "none")
            }
        }

        return AddUserChange(
            userName = "user${counter.incrementAndGet()}",
            peersets = peersets,
            notificationUrl = "$ownAddress/api/v1/notification",
        )
    }

}
