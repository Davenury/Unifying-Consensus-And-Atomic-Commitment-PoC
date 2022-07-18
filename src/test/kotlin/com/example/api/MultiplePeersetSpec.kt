package com.example.api

import com.example.*
import com.example.common.AddUserChange
import com.example.common.ChangeDto
import com.example.consensus.ratis.ChangeWithAcceptNum
import com.example.consensus.ratis.ChangeWithAcceptNumDto
import com.example.consensus.ratis.HistoryDto
import com.example.gpac.domain.Accept
import com.example.gpac.domain.Apply
import com.example.gpac.domain.Transaction
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo
import java.io.File

class MultiplePeersetSpec {

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {

        // given - applications
        val app1 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "1"), emptyMap()) }
        val app2 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "1"), emptyMap()) }
        val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }
        val app4 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "2"), emptyMap()) }
        val app5 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "2"), emptyMap()) }
        val app6 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        executeChange("$peer1/create_change")

        // then - transaction is executed in same peerset
        val peer2Change = httpClient.get<String>("$peer2/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer2Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - transaction is executed in other peerset
        val peer4Change = httpClient.get<String>("$peer4/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer4Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - there's only one change in history of both peersets
        askForChanges(peer2)
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges(peer4)
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        listOf(app1, app2, app3, app4, app5, app6).forEach { app -> app.cancel() }
    }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit = runBlocking {

        // given - applications
        val app1 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "1"), emptyMap()) }
        val app2 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "1"), emptyMap()) }
        val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        // val app4 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "2"), emptyMap()) }
        // val app5 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "2"), emptyMap()) }
        // val app6 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        try {
            executeChange("$peer1/create_change")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message).isEqualTo("""Server error(http://localhost:8081/create_change: 503 Service Unavailable. Text: "{"msg":"Transaction failed due to too many retries of becoming a leader."}"""")
        }

        // we need to wait for timeout from peers of second peerset
        delay(15000)

        // then - transaction should not be executed
        askForChanges(peer3)
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        listOf(app1, app2, app3).forEach { app -> app.cancel() }
    }

    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {

        val configOverrides = mapOf(
            "peers.peersAddresses" to listOf(createPeersInRange(3), createPeersInRange(4, 3)),
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${it + 11124}" },
                List(5) { "localhost:${it + 11134}" })
        )

        // given - applications
        val app1 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app2 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        //val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app5 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        // val app6 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "2"), emptyMap()) }
        // val app7 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "2"), emptyMap()) }
        // val app8 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("5", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        try {
            executeChange("$peer1/create_change")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message).isEqualTo("""Server error(http://localhost:8081/create_change: 503 Service Unavailable. Text: "{"msg":"Transaction failed due to too many retries of becoming a leader."}"""")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askForChanges(peer2)
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        listOf(app1, app2, app4, app5).forEach { app -> app.cancel() }
    }

    @Test
    fun `transaction should pass when more than half peers of all peersets are operative`(): Unit = runBlocking {

        val configOverrides = mapOf(
            "peers.peersAddresses" to listOf(createPeersInRange(3), createPeersInRange(5, 3)),
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${it + 11124}" },
                List(5) { "localhost:${it + 11134}" })
        )

        // given - applications
        val app1 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app2 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        //val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app5 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app6 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("3", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        // val app7 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "2"), emptyMap()) }
        // val app8 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("5", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        executeChange("$peer1/create_change")

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should be executed in every peerset
        askForChanges(peer2)
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges(peer4)
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        listOf(app1, app2, app4, app5, app6).forEach { app -> app.cancel() }
    }

    @Test
    fun `transaction should not be processed if every peer from one peerset fails after ft-agree`(): Unit =
        runBlocking {
            val configOverrides = mapOf(
                "peers.peersAddresses" to listOf(createPeersInRange(3), createPeersInRange(5, 3)),
                "raft.server.addresses" to listOf(
                    List(3) { "localhost:${it + 11124}" },
                    List(5) { "localhost:${it + 11134}" })
            )

            val failAction: suspend (Transaction?) -> Unit = {
                throw RuntimeException()
            }

            // given - applications
            val app1 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("1", "1"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app2 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("2", "1"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app3 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("3", "1"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }

            // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
            val app4 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("1", "2"),
                    mapOf(TestAddon.OnHandlingAgreeEnd to failAction),
                    configOverrides = configOverrides
                )
            }
            val app5 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("2", "2"),
                    mapOf(TestAddon.OnHandlingAgreeEnd to failAction),
                    configOverrides = configOverrides
                )
            }
            val app6 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("3", "2"),
                    mapOf(TestAddon.OnHandlingAgreeEnd to failAction),
                    configOverrides = configOverrides
                )
            }
            val app7 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("4", "2"),
                    mapOf(TestAddon.OnHandlingAgreeEnd to failAction),
                    configOverrides = configOverrides
                )
            }
            val app8 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("5", "2"),
                    mapOf(TestAddon.OnHandlingAgreeEnd to failAction),
                    configOverrides = configOverrides
                )
            }

            delay(5000)

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("$peer1/create_change")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message).isEqualTo("""Server error(http://localhost:8081/create_change: 503 Service Unavailable. Text: "{"msg":"Transaction failed due to too few responses of ft phase."}"""")
            }

            allPeers.forEach {
                askForChanges(it)
                    .let {
                        expect {
                            that(it.size).isEqualTo(0)
                        }
                    }
            }

            listOf(app1, app2, app3, app4, app5, app6, app7, app8).forEach { app -> app.cancel() }
        }

    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit = runBlocking {
        val configOverrides = mapOf(
            "peers.peersAddresses" to listOf(createPeersInRange(3), createPeersInRange(5, 3)),
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${it + 11124}" },
                List(5) { "localhost:${it + 11134}" })
        )

        val failAction: suspend (Transaction?) -> Unit = {
            throw RuntimeException()
        }

        // given - applications
        val app1 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "1"),
                mapOf(TestAddon.BeforeSendingApply to failAction),
                configOverrides = configOverrides
            )
        }
        val app2 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app3 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("3", "1"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("1", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app5 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("2", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app6 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("3", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app7 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("4", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }
        val app8 = GlobalScope.launch(Dispatchers.IO) {
            startApplication(
                arrayOf("5", "2"),
                emptyMap(),
                configOverrides = configOverrides
            )
        }

        delay(5000)

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("$peer1/create_change")
        }

        // application should elect recovery leader to perform transaction to the end
        delay(20000)

        allPeers.forEach {
            askForChanges(it)
                .let {
                    expect {
                        that(it.size).isGreaterThanOrEqualTo(1)
                        that(it[0].change).isEqualTo(AddUserChange("userName"))
                    }
                }
        }

        listOf(app1, app2, app3, app4, app5, app6, app7, app8).forEach { app -> app.cancel() }
    }

    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {
            val configOverrides = mapOf(
                "peers.peersAddresses" to listOf(createPeersInRange(3), createPeersInRange(5, 3)),
                "raft.server.addresses" to listOf(
                    List(3) { "localhost:${it + 11124}" },
                    List(5) { "localhost:${it + 11134}" })
            )

            val leaderAction: suspend (Transaction?) -> Unit = {
                val url2 = "$peer2/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(it!!.ballotNumber, true, Accept.COMMIT, ChangeDto(mapOf(
                            "operation" to "ADD_USER",
                            "userName" to "userName"
                        ))
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("Localhost 8082 sent response to apply")
                val url3 = "$peer3/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url3) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(it!!.ballotNumber, true, Accept.COMMIT, ChangeDto(mapOf(
                            "operation" to "ADD_USER",
                            "userName" to "userName"
                        ))
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("Localhost 8083 sent response to apply")
                throw RuntimeException()
            }

            // given - applications
            val app1 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("1", "1"),
                    mapOf(TestAddon.BeforeSendingApply to leaderAction),
                    configOverrides = configOverrides
                )
            }
            val app2 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("2", "1"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app3 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("3", "1"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }

            // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
            val app4 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("1", "2"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app5 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("2", "2"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app6 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("3", "2"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app7 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("4", "2"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }
            val app8 = GlobalScope.launch(Dispatchers.IO) {
                startApplication(
                    arrayOf("5", "2"),
                    emptyMap(),
                    configOverrides = configOverrides
                )
            }

            delay(5000)

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChange("$peer1/create_change")
            }

            // application should elect recovery leader to perform transaction to the end
            delay(20000)

            allPeers.forEach {
                askForChanges(it)
                    .let {
                        expect {
                            that(it.size).isGreaterThanOrEqualTo(1)
                            that(it[0].change).isEqualTo(AddUserChange("userName"))
                        }
                    }
            }

            listOf(app1, app2, app3, app4, app5, app6, app7, app8).forEach { app -> app.cancel() }
        }


    private val peer1 = "http://localhost:8081"
    private val peer2 = "http://localhost:8082"
    private val peer3 = "http://localhost:8083"
    private val peer4 = "http://localhost:8084"
    private val peer5 = "http://localhost:8085"
    private val peer6 = "http://localhost:8086"
    private val peer7 = "http://localhost:8087"
    private val peer8 = "http://localhost:8088"
    private val allPeers = listOf(peer1, peer2, peer3, peer4, peer5, peer6, peer7, peer8)

    private suspend fun executeChange(uri: String, change: Map<String, Any> = changeDto.properties) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peer: String) =
        testHttpClient.get<String>("$peer/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.let { objectMapper.readValue<HistoryDto>(it) }
            .changes.map { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

    private fun createPeersInRange(range: Int, offset: Int = 0): List<String> =
        List(range) { "localhost:${8081 + it + offset}" }

    private val changeDto = ChangeDto(
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }

}
