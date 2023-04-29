package com.github.davenury.ucac.consensus.paxos

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*


interface PigPaxosProtocolClient {

    suspend fun sendProposes(
        peers: List<PeerAddress>,
        message: PaxosPropose
    ): List<ConsensusResponse<PaxosPromise?>>

    suspend fun sendAccept(
        peer: PeerAddress,
        message: PaxosAccept
    ): ConsensusResponse<PaxosAccepted?>


    suspend fun sendCommit(
        peer: PeerAddress,
        message: PaxosCommit
    ): ConsensusResponse<String?>

    suspend fun sendRequestApplyChange(
        peer: PeerAddress,
        change: Change
    ): ChangeResult
}

class PigPaxosProtocolClientImpl(override val peersetId: PeersetId) : PigPaxosProtocolClient, ConsensusProtocolClient(peersetId) {

    override suspend fun sendProposes(
        peers: List<PeerAddress>,
        message: PaxosPropose
    ): List<ConsensusResponse<PaxosPromise?>> {
        logger.debug("Sending proposes requestes to ${peers.map { it.peerId }}")
        return peers
            .map { Pair(it, message) }
            .let { sendRequests(it, "paxos/propose") }
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: PaxosAccept
    ): ConsensusResponse<PaxosAccepted?> {
        logger.debug("Sending accept request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "paxos/accept")
    }

    override suspend fun sendCommit(peer: PeerAddress, message: PaxosCommit): ConsensusResponse<String?> {
        logger.debug("Sending commit request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "paxos/commit")
    }

    override suspend fun sendRequestApplyChange(peer: PeerAddress, change: Change): ChangeResult =
        httpClient.post("http://${peer.address}/paxos/request_apply_change?peerset=${peersetId}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }
}