package com.example

import com.example.api.configureRouting
import com.example.domain.MissingParameterException
import com.example.domain.UnknownOperationException
import com.example.raft.Constants
import com.example.raft.RaftNode
import com.sksamuel.hoplite.ConfigLoader
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftPeer
import org.koin.core.qualifier.named
import org.koin.dsl.module
import org.koin.ktor.ext.Koin
import org.koin.ktor.ext.inject
import java.io.File
import java.util.UUID

fun main(args: Array<String>) {

    val id = args[0].toInt()

    embeddedServer(Netty, port = 8080+id, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
        }

        val config = loadConfig()
        config.raft.server.addresses.let { addresses ->
            require(addresses.isNotEmpty()) { "Failed to get addresses from $config" }
        }
        val configModule = module {
            val peers: List<RaftPeer> = config.raft.server.addresses.mapIndexed { index, address -> RaftPeer.newBuilder().setId("n$index").setAddress(address).build() }
            single { peers }

            single<UUID>(named("clusterGroupId")) { UUID.fromString(config.raft.clusterGroupId) }

            single<String>(named("path")) { config.raft.server.root.storage.path }

            single<RaftGroup> { RaftGroup.valueOf(
                RaftGroupId.valueOf(get<UUID>(named("clusterGroupId"))),
                get<List<RaftPeer>>()
            ) }

            single<RaftNode> {
                RaftNode(peers[id - 1], File("./$id"), get())
            }
        }

        install(Koin) {
            modules(historyManagementModule, configModule)
        }

        install(StatusPages) {
            exception<MissingParameterException> { cause ->
                call.respondText(status = HttpStatusCode.BadRequest, text = "Missing parameter: ${cause.message}")
            }
            exception<UnknownOperationException> { cause ->
                call.respondText(
                    status = HttpStatusCode.BadRequest,
                    text = "Unknown operation to perform: ${cause.desiredOperationName}"
                )
            }
        }

        val raftNode: RaftNode by inject()
        configureRouting(raftNode)
    }.start(wait = true)
}
