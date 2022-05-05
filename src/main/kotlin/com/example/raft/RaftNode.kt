package com.example.raft

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.*
import java.nio.charset.Charset;

/**
 * Simplest Ratis server, use a simple state machine [CounterStateMachine]
 * which maintain a counter across multi server.
 * This server application designed to run several times with different
 * parameters (1,2 or 3). server addresses hard coded in [Constants]
 *
 *
 * Run this application three times with three different parameter set-up a
 * ratis cluster which maintain a counter value replicated in each server memory
 */
class RaftNode(
    peer: RaftPeer,
    storageDir: File?,
    raftGroup: RaftGroup
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
        val counterStateMachine = CounterStateMachine()

        //create and start the Raft server
        server = RaftServer.newBuilder()
            .setGroup(raftGroup)
            .setProperties(properties)
            .setServerId(peer.id)
            .setStateMachine(counterStateMachine)
            .build()

        println(raftGroup)
        println(peer.address)

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

    fun getValue(): String {
        val count: RaftClientReply = client.io().sendReadOnly(Message.valueOf("GET"))
        return count.message.content.toString(Charset.defaultCharset());
    }

    fun incrementValue() {
        client.io().send(Message.valueOf("INCREMENT"))
    }
}