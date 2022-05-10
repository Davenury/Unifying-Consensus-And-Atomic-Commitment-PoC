package com.example

import com.example.domain.RaftPeerDto
import com.example.raft.Constants
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse


class ConfigStore(args: Array<String>) {

    val nodeId: Int
    val insidePort: Int
    private val outsidePort: Int
    val peer: RaftPeerDto
    val peers: List<RaftPeerDto>
    private val startPortNumber: Int = 8080

    init {
        val conf = getIdAndOffset(args)
        nodeId = conf.first
        insidePort = startPortNumber + conf.second
        outsidePort = startPortNumber + conf.first
        val configRaftPeer = Constants.PEERS_DTO.getOrNull(nodeId)
        if (configRaftPeer == null) {
            val leaderResponse = askLeader(nodeId)
            peer = leaderResponse.first
            informPeers(peer, leaderResponse.second)
            peers = leaderResponse.second.plus(peer)
        } else {
            peer = configRaftPeer
            peers = Constants.PEERS_DTO
        }
    }


    private fun askLeader(nodeId: Int): Pair<RaftPeerDto, List<RaftPeerDto>> {

        val response = sendToRunningLeader { leader ->
            HttpRequest.newBuilder()
                .uri(URI.create("http://${leader}/config"))
                .build()
        } ?: throw Exception("None of config servers is running")

        val peers: List<RaftPeerDto> = objectMapper
            .readValue(response.body(), ArrayList<HashMap<String, String>>().javaClass)
            .map { RaftPeerDto.fromJson(it) }

        val allPorts = peers.map { it.getPort() }

        val newPort = findPort(allPorts)

        val newPeer =
            RaftPeerDto(nodeId.toString(), localhostAddress(newPort), localhostAddress(outsidePort))
        return Pair(newPeer, peers)
    }

    private fun sendToRunningLeader(fn: (address: String) -> HttpRequest): HttpResponse<String>? {
        for (leader in Constants.PEERS_DTO) {
            val request = fn(leader.httpAddress)
            val response = tryCatchSend(request, leader.address)
            if (response != null && response.statusCode() == 200) return response
        }
        return null
    }

    private fun localhostAddress(portNum: Int): String = "127.0.0.1:$portNum"

    private fun findPort(allPorts: List<Int>): Int = findPort(allPorts.last() + 1, allPorts)

    private fun findPort(portNum: Int, allPorts: List<Int>): Int =
        if (allPorts.contains(portNum)) findPort(portNum + 1, allPorts)
        else portNum

    private fun informPeers(raftPeerDto: RaftPeerDto, peers: List<RaftPeerDto>) {
        var anyAccept = false
        for (leader in peers) {
            val request = HttpRequest
                .newBuilder()
                .uri(URI.create("http://${leader.httpAddress}/add_peer"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(raftPeerDto)))
                .build()
            val response = tryCatchSend(request, leader.httpAddress)
            if (response?.statusCode() != 200) logger.error("Leader $leader response with $response\n ${response?.body()?:"Nothing"}")
            if(response?.statusCode() == 200)anyAccept = true
        }
        if(!anyAccept) throw Exception("All peers rejected me")
    }

    private fun tryCatchSend(request: HttpRequest, address: String): HttpResponse<String>? =
        try {
            client.send(request, HttpResponse.BodyHandlers.ofString())
        } catch (e: ConnectException) {
            logger.error("Address $address is closed")
            null
        }

    private fun getIdAndOffset(args: Array<String>): Pair<Int, Int> =
        if (args.isNotEmpty()) Pair(args[0].toInt(), args[0].toInt())
        else System
            .getenv()["RAFT_NODE_ID"]
            ?.toInt()
            ?.let { Pair(it, 0) }
            ?: throw RuntimeException("Provide either arg or RAFT_NODE_ID env variable to represent id of node")

    companion object {
        val logger = LoggerFactory.getLogger(this::class.java)
    }
}

