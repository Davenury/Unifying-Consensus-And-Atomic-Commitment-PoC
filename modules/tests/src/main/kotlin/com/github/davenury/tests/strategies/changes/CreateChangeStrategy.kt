package com.github.davenury.tests.strategies.changes

import com.github.davenury.common.Change
import com.github.davenury.common.PeersetId
import com.github.davenury.tests.OnePeersetChanges

interface CreateChangeStrategy {
    fun createChange(ids: List<PeersetId>, changes: Map<PeersetId, OnePeersetChanges>): Change
}
