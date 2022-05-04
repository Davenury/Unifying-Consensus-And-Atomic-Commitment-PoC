package com.example.raft

import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftPeer
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.*

/*
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



/**
 * Constants across servers and clients
 */
object Constants {
    lateinit var PEERS: List<RaftPeer>
    lateinit var PATH: String

    fun rootPath() = Paths.get("").toAbsolutePath().toString()

    init {
        val properties = Properties()
        val conf = Paths.get(rootPath(), "/src/main/resources/conf.properties").toString()
        print(conf)
        try {
            FileInputStream(conf).use { inputStream ->
                InputStreamReader(inputStream, StandardCharsets.UTF_8).use { reader ->
                    BufferedReader(reader).use { bufferedReader ->
                        properties.load(bufferedReader)
                    }
                }
            }
        } catch (e: IOException) {
            throw IllegalStateException("Failed to load $conf", e)
        }
        val key = "raft.server.address.list"
        val addresses = Optional.ofNullable(properties.getProperty(key))
            .map { s: String ->
                s.split(
                    ","
                ).toTypedArray()
            }
            .orElse(null)
        require(!(addresses == null || addresses.size == 0)) { "Failed to get $key from $conf" }
        val key1 = "raft.server.root.storage.path"
        val path = properties.getProperty(key1)
        PATH = path ?: "./raft-examples/target"
        val peers: MutableList<RaftPeer> = ArrayList(addresses.size)
        for (i in addresses.indices) {
            peers.add(RaftPeer.newBuilder().setId("n$i").setAddress(addresses[i]).build())
        }
        PEERS = Collections.unmodifiableList(peers)
    }

    private val CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1")
    val RAFT_GROUP = RaftGroup.valueOf(
        RaftGroupId.valueOf(CLUSTER_GROUP_ID), PEERS
    )

    fun oneNodeGroup(peer: RaftPeer): RaftGroup {
        return RaftGroup.valueOf(
            RaftGroupId.valueOf(CLUSTER_GROUP_ID), peer
        )
    }
}