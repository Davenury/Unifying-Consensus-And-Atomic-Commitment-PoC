package com.github.davenury.ucac.consensus.ratis

import com.github.davenury.ucac.RatisConfig
import org.apache.ratis.client.RaftClient
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.GrpcFactory
import org.apache.ratis.protocol.*
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.util.NetUtils
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.nio.charset.Charset
import java.util.*

abstract class RatisNode(
    peerId: Int,
    stateMachine: StateMachine<*>,
    storageDir: File?,
    peersetId: Int,
    config: RatisConfig
) : Closeable {
    private val server: RaftServer
    private val client: RaftClient
    private val peer: RaftPeer
    private val clusterGroupId: UUID

    init {
        //create a property object
        val properties = RaftProperties()

        clusterGroupId = UUID(0, peersetId.toLong())
        val peers = config.peerAddresses()
            .filter { it.key.peersetId == peersetId }
            .map {
                RaftPeer.newBuilder()
                    .setId("n${it.key}")
                    .setAddress(it.value.address)
                    .build()
            }
        peer = peers[peerId]

        //set the storage directory (different for each peer) in RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, listOf(storageDir))

        //set the port which server listen to in RaftProperty object
        val port: Int = NetUtils.createSocketAddr(peer.address).port
        GrpcConfigKeys.Server.setPort(properties, port)

        //create the counter state machine which hold the counter value

        //create and start the Raft server
        server = RaftServer.newBuilder()
            .setGroup(RaftGroup.valueOf(RaftGroupId.valueOf(clusterGroupId), peers))
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
            .setRaftGroup(
                RaftGroup.valueOf(
                    RaftGroupId.valueOf(clusterGroupId), peer
                )
            )
            .setClientRpc(
                GrpcFactory(Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties)
            )
        return builder.build()
    }

    fun applyTransaction(msg: String): String = msg
        .let { Message.valueOf(it) }
        .let { client.io().send(it) }
        .message
        .content
        .toString(Charset.defaultCharset())
}
