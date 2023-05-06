package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.PeerId
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap

class VotesContainer {

    private val entryIdToVotesCommit: ConcurrentHashMap<String, List<PeerId>> = ConcurrentHashMap()
    private val entryIdToVotesAbort: ConcurrentHashMap<String, List<PeerId>> = ConcurrentHashMap()
    private val mutex = Mutex()

    public suspend fun initializeEntry(entryId: String) = mutex.withLock {
        initializeEntryIfAbsent(entryId)
    }

    private fun initializeEntryIfAbsent(entryId: String){
        entryIdToVotesCommit.putIfAbsent(entryId, listOf())
        entryIdToVotesAbort.putIfAbsent(entryId, listOf())
    }

    public suspend fun voteOnEntry(entryId: String, vote: AlvinResult, peerId: PeerId) = mutex.withLock {

        val container = if (vote == AlvinResult.COMMIT) entryIdToVotesCommit else entryIdToVotesAbort

        container[entryId] = (container.getOrDefault(entryId, listOf()) + listOf(peerId)).distinct()
    }

    public suspend fun getVotes(entryId: String, isVotingForCommit: Boolean): Pair<Int, Int> = mutex.withLock {
        initializeEntryIfAbsent(entryId)
        val (commitModifier, abortModifier) = if (isVotingForCommit) Pair(0, -1) else Pair(-1, 0)
        Pair(entryIdToVotesCommit[entryId]!!.size + commitModifier, entryIdToVotesAbort[entryId]!!.size + abortModifier)
    }

    public suspend fun removeEntry(entryId: String) = mutex.withLock {
        entryIdToVotesCommit.remove(entryId)
        entryIdToVotesAbort.remove(entryId)
    }


}