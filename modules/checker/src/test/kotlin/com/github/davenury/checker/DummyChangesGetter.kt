package com.github.davenury.checker

import com.github.davenury.common.Changes
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId

class DummyChangesGetter(
    private val changes: Map<PeerAddress, Changes>
): ChangesGetter {

    override suspend fun getChanges(address: PeerAddress, peersetId: PeersetId): Changes = changes[address]!!
}