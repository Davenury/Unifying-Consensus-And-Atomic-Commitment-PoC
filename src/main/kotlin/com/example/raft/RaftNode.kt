package com.example.raft

import org.apache.ratis.client.RaftClient
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.GrpcFactory
import org.apache.ratis.protocol.*
import org.apache.ratis.rpc.CallId
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.util.NetUtils
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.nio.charset.Charset

abstract class RaftNode(
    val peer: RaftPeer,
    private val stateMachine: StateMachine<*>,
    private var peers: List<RaftPeer>,
    storageDir: File? = File("./history-${peer.id}")
) : Closeable {
    private var server: RaftServer

    private var client: RaftClient

    private val properties: RaftProperties = RaftProperties()


    init {
        //create a property object

        //set the storage directory (different for each peer) in RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, listOf(storageDir))

        //set the port which server listen to in RaftProperty object
        val port: Int = NetUtils.createSocketAddr(peer.address).port
        GrpcConfigKeys.Server.setPort(properties, port)

        //create the counter state machine which hold the counter value

        //create and start the Raft server
        server = buildServer(peer, peers)

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
        return RaftClient.newBuilder()
            .setProperties(raftProperties)
            .setRaftGroup(Constants.oneNodeGroup(peer))
            .setClientRpc(
                GrpcFactory(Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties)
            )
            .build()
    }

    private fun buildServer(peer: RaftPeer, peers: List<RaftPeer>): RaftServer =
        RaftServer.newBuilder()
            .setGroup(Constants.createRaftGroups(peers))
            .setProperties(properties)
            .setServerId(peer.id)
            .setStateMachine(stateMachine)
            .build()

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

    fun newPeerSet(peers: List<RaftPeer>) {
        close()
        server = buildServer(peer,peers)
        start()
//        val groupId = client.getGroupManagementApi(server.id).list().groupIds.first()
//
//        val reply = client.clientRpc.sendRequest(
//            SetConfigurationRequest(
//                client.id,
//                client.leaderId,
//                groupId,
//                CallId.getAndIncrement(),
//                peers
//            )
//        )
//
//        println(reply)
    }

//    fun addPeer(peer: RaftPeer) {
//        val groupId = client.getGroupManagementApi(server.id).list().groupIds.first()
//        val reply = server.groupManagement(
//            GroupManagementRequest
//                .newAdd(client.id, server.id, CallId.getAndIncrement(), Constants.oneNodeGroup(peer))
//        )
//        val reply = client.getGroupManagementApi(server.id).add(Constants.oneNodeGroup(peer))
//        println(reply)
//    }

}