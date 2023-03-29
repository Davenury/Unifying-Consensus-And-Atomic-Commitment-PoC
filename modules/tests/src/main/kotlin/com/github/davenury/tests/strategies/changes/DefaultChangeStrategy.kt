package com.github.davenury.tests.strategies.changes

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.PeersetId
import com.github.davenury.tests.OnePeersetChanges
import java.util.concurrent.atomic.AtomicInteger

class DefaultChangeStrategy(
    private val ownAddress: String
): CreateChangeStrategy {

    private var counter = AtomicInteger(0)

    override fun createChange(ids: List<PeersetId>, changes: Map<PeersetId, OnePeersetChanges>, changeId: String): Change =
        AddUserChange(
            id = changeId,
            userName = "user${counter.incrementAndGet()}",
            peersets = ids.map { ChangePeersetInfo(it, changes[it]!!.getCurrentParentId()) },
            notificationUrl = "$ownAddress/api/v1/notification",
        )
}
