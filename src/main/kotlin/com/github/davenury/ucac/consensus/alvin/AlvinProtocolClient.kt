package com.github.davenury.ucac.consensus.alvin

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusProtocolClientImpl
import com.github.davenury.ucac.consensus.ConsensusResponse
import org.slf4j.LoggerFactory



interface AlvinProtocolClient: ConsensusProtocolClient {

    suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?>

    suspend fun sendAccept(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinAckAccept?>

    suspend fun sendStable(
        peer: PeerAddress,
        message: AlvinStable,
    ): ConsensusResponse<AlvinAckStable?>

    suspend fun sendPrepare(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinPromise?>


    suspend fun sendCommit(
        peer: PeerAddress,
        message: AlvinCommit
    ): ConsensusResponse<AlvinCommitResponse?>

    suspend fun sendFastRecovery(
        peer: PeerAddress,
        message: AlvinFastRecovery
    ): ConsensusResponse<AlvinFastRecoveryResponse?>
}

public class AlvinProtocolClientImplImpl(override val peersetId: PeersetId) : AlvinProtocolClient, ConsensusProtocolClientImpl(peersetId) {

    override suspend fun sendProposal(
        peer: PeerAddress,
        message: AlvinPropose
    ): ConsensusResponse<AlvinAckPropose?> {
        logger.debug("Sending proposal request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/proposal")
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: AlvinAccept
    ): ConsensusResponse<AlvinAckAccept?> {
        logger.debug("Sending accept request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/accept")
    }

    override suspend fun sendStable(peer: PeerAddress, message: AlvinStable): ConsensusResponse<AlvinAckStable?> {
        logger.debug("Sending stable request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/stable")
    }

    override suspend fun sendPrepare(peer: PeerAddress, message: AlvinAccept): ConsensusResponse<AlvinPromise?> {
        logger.debug("Sending prepare request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/prepare")
    }

    override suspend fun sendCommit(peer: PeerAddress, message: AlvinCommit): ConsensusResponse<AlvinCommitResponse?> {
        logger.debug("Sending commit request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/commit")
    }

    override suspend fun sendFastRecovery(
        peer: PeerAddress,
        message: AlvinFastRecovery
    ): ConsensusResponse<AlvinFastRecoveryResponse?> {
        logger.debug("Sending fastRecovery request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "alvin/fast-recovery")
    }

    companion object {
        private val logger = LoggerFactory.getLogger("alvin-client")
    }
}