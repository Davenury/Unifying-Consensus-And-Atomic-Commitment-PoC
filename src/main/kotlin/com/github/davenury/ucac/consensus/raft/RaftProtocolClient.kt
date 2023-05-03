package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.httpClient
import com.github.davenury.ucac.raftHttpClient
import com.zopa.ktor.opentracing.asyncTraced
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<ConsensusResponse<ConsensusElectedYou?>>

    suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
    ): ConsensusResponse<ConsensusHeartbeatResponse?>


    suspend fun sendRequestApplyChange(
        address: String,
        change: Change
    ): ChangeResult
}

class RaftProtocolClientImpl(override val peersetId: PeersetId) : RaftProtocolClient, ConsensusProtocolClient(peersetId) {
    override suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe
    ): List<ConsensusResponse<ConsensusElectedYou?>> {
        logger.debug("Sending elect me requests to ${otherPeers.map { it.peerId }}")
        return otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "raft/request_vote") }
    }

    override suspend fun sendConsensusHeartbeat(
        peer: PeerAddress, message: ConsensusHeartbeat,
    ): ConsensusResponse<ConsensusHeartbeatResponse?> {

        return CoroutineScope(Dispatchers.IO).asyncTraced(MDCContext()) {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(peer, "raft/heartbeat", message)
        }.let {
            val result = try {
                it.await()
            } catch (e: Exception) {
                logger.error("Error while evaluating response from ${peer.peerId}", e)
                null
            }
            ConsensusResponse(peer.address, result)
        }
    }

    override suspend fun sendRequestApplyChange(address: String, change: Change) =
        httpClient.post<ChangeResult>("http://${address}/raft/request_apply_change?peerset=${peersetId}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }



    companion object {
        private val logger = LoggerFactory.getLogger("raft-client")
    }
}