package com.example.raft

import com.example.loadConfig
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftPeer
import java.util.*


/**
 * Constants across servers and clients
 */
object Constants {
    val PEERS: List<RaftPeer>
    val PATH: String
    val CLUSTER_GROUP_ID: UUID
    val RAFT_GROUP: RaftGroup

    init {
        val config = loadConfig("/${System.getenv("CONFIG_FILE") ?: "application.conf"}")
        PATH = config.raft.server.root.storage.path
        PEERS = config.raft.server.addresses.mapIndexed { index, address ->
            RaftPeer.newBuilder().setId("n$index").setAddress(address).build()
        }
        CLUSTER_GROUP_ID = UUID.fromString(config.raft.clusterGroupId)
        RAFT_GROUP = createRaftGroups()
    }

    fun oneNodeGroup(peer: RaftPeer): RaftGroup {
        return RaftGroup.valueOf(
            RaftGroupId.valueOf(CLUSTER_GROUP_ID), peer
        )
    }

    fun createRaftGroups(peers: List<RaftPeer> = PEERS): RaftGroup =
        RaftGroup.valueOf(RaftGroupId.valueOf(CLUSTER_GROUP_ID), peers)
}