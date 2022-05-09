package com.example.raft

import org.apache.ratis.client.RaftClient
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.GrpcFactory
import org.apache.ratis.protocol.ClientId
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftPeer
import org.apache.ratis.protocol.SetConfigurationRequest
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.util.NetUtils
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.nio.charset.Charset

abstract class RaftNode(
    peer: RaftPeer,
    stateMachine: StateMachine<*>,
    peers: List<RaftPeer> = Constants.PEERS,
    storageDir: File? = File("./history-${peer.id}")
) : Closeable {
    private val server: RaftServer

    private val client: RaftClient

    init {
        //create a property object
        val properties = RaftProperties()

        //set the storage directory (different for each peer) in RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, listOf(storageDir))

        //set the port which server listen to in RaftProperty object
        val port: Int = NetUtils.createSocketAddr(peer.address).port
        GrpcConfigKeys.Server.setPort(properties, port)

        //create the counter state machine which hold the counter value

        //create and start the Raft server
        server = RaftServer.newBuilder()
            .setGroup(Constants.createRaftGroups(peers))
            .setProperties(properties)
            .setServerId(peer.id)
            .setStateMachine(stateMachine)
            .build()


        client = buildClient(peer)

        this.start()
    }


    @Throws(IOException::class)
    fun start() {
        server.start()
    }

    @Throws(IOException::class)
    override fun close() {
        server.close()
    }


    private fun buildClient(peer: RaftPeer): RaftClient {
        val raftProperties = RaftProperties()
        val builder = RaftClient.newBuilder()
            .setProperties(raftProperties)
            .setRaftGroup(Constants.oneNodeGroup(peer))
            .setClientRpc(
                GrpcFactory(Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties)
            )
        return builder.build()
    }

    fun queryData(msg: String): String = msg
        .let { Message.valueOf(it) }
        .let { client.io().sendReadOnly(it) }
        .message
        .content
        .toString(Charset.defaultCharset());

    fun applyTransaction(msg: String): String = msg
        .let { Message.valueOf(it) }
        .let { client.io().send(it) }
        .message
        .content
        .toString(Charset.defaultCharset());

    fun getPeersGroups(): List<RaftPeer> =
        server.groups.flatMap { it.peers }

    fun addPeer(peer: RaftPeer) {
        val newPeers = server.groups.flatMap { it.peers }.toSet().plus(peer).toList()
        val serverConfiguration = SetConfigurationRequest(client.id, server.id, server.groupIds.first(), 1, newPeers)
        server.setConfiguration(serverConfiguration)
    }
}