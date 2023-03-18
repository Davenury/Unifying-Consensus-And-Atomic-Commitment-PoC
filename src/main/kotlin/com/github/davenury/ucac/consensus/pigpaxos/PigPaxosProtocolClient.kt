package com.github.davenury.ucac.consensus.pigpaxos

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.common.PeerAddress
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.httpClient
import io.ktor.client.request.*
import io.ktor.http.*


interface PigPaxosProtocolClient {

    suspend fun sendPropose(
        peer: PeerAddress,
        message: PaxosPropose
    ): ConsensusResponse<PaxosPromise?>

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

class PigPaxosProtocolClientImpl() : PigPaxosProtocolClient, ConsensusProtocolClient() {

    override suspend fun sendPropose(peer: PeerAddress, message: PaxosPropose): ConsensusResponse<PaxosPromise?> {
        logger.debug("Sending proposal request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "pigpaxos/propose")
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: PaxosAccept
    ): ConsensusResponse<PaxosAccepted?> {
        logger.debug("Sending accept request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "pigpaxos/accept")
    }

    override suspend fun sendCommit(peer: PeerAddress, message: PaxosCommit): ConsensusResponse<String?> {
        logger.debug("Sending commit request to ${peer.globalPeerId}")
        return sendRequest(Pair(peer, message), "pigpaxos/commit")
    }

    override suspend fun sendRequestApplyChange(peer: PeerAddress, change: Change): ChangeResult =
        httpClient.post("http://${peer.address}/pigpaxos/request_apply_change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }
}