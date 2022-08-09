package com.github.davenury.ucac.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.ChangeDto
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNumDto
import com.github.davenury.ucac.consensus.ratis.HistoryDto
import com.github.davenury.ucac.gpac.domain.Accept
import com.github.davenury.ucac.gpac.domain.Apply
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.contains
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThanOrEqualTo
import java.io.File
import java.util.*
import kotlin.random.Random

class MultiplePeersetSpec {

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
        deleteRaftHistories()
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit = runBlocking {
        val numberOfApps = 6
        val ratisPorts = List(numberOfApps) { Random.nextInt(10000, 20000) + it }

        val configOverrides = mapOf<String, Any>(
            "raft.server.addresses" to ratisPorts.chunked(3).map { it.map { "localhost:${it + 11124}" } },
            "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
        )

        // given - applications
        val apps = listOf(
            createApplication(arrayOf("1", "1"), emptyMap(), mode = TestApplicationMode(1, 1), configOverrides = configOverrides),
            createApplication(arrayOf("2", "1"), emptyMap(), mode = TestApplicationMode(2, 1), configOverrides = configOverrides),
            createApplication(arrayOf("3", "1"), emptyMap(), mode = TestApplicationMode(3, 1), configOverrides = configOverrides),
            createApplication(arrayOf("1", "2"), emptyMap(), mode = TestApplicationMode(1, 2), configOverrides = configOverrides),
            createApplication(arrayOf("2", "2"), emptyMap(), mode = TestApplicationMode(2, 2), configOverrides = configOverrides),
            createApplication(arrayOf("3", "2"), emptyMap(), mode = TestApplicationMode(3, 2), configOverrides = configOverrides),
        )
        apps.forEach { app -> app.startNonblocking() }
        val peers = apps.chunked(3).map { it.map { "localhost:${it.getBoundPort()}" } }
        apps.forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }

        // when - executing transaction
        executeChange("http://${peers[0][0]}/create_change")

        // then - transaction is executed in same peerset
        val peer2Change = httpClient.get<String>("http://${peers[0][1]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer2Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - transaction is executed in other peerset
        val peer4Change = httpClient.get<String>("http://${peers[1][0]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
            .let { objectMapper.readValue<ChangeWithAcceptNumDto>(it) }
            .let { ChangeWithAcceptNum(it.change.toChange(), it.acceptNum) }

        expect {
            that(peer4Change.change).isEqualTo(AddUserChange("userName"))
        }

        // and - there's only one change in history of both peersets
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges("http://${peers[1][0]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        apps.forEach { app -> app.stop() }
    }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit = runBlocking {

        val numberOfApps = 6
        val ratisPorts = List(numberOfApps) { Random.nextInt(10000, 20000) + it }

        val configOverrides = mapOf<String, Any>(
            "raft.server.addresses" to ratisPorts.chunked(3).map { it.map { "localhost:${it + 11124}" } },
            "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
        )

        // given - applications
        val apps = listOf(
            createApplication(arrayOf("1", "1"), emptyMap(), configOverrides = configOverrides, mode = TestApplicationMode(1, 1)),
            createApplication(arrayOf("2", "1"), emptyMap(), configOverrides = configOverrides, mode = TestApplicationMode(2, 1)),
            createApplication(arrayOf("3", "1"), emptyMap(), configOverrides = configOverrides, mode = TestApplicationMode(3, 1))
        )
        apps.forEach { app -> app.startNonblocking() }
        val peers = apps.chunked(3).map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList()

        //since we're not starting application, we need to assure that addresses of those unstarted applications will be set
        peers.add(listOf("localhost:8084", "localhost:8085", "localhost:8086"))
        apps.forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        // val app4 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("1", "2"), emptyMap()) }
        // val app5 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("2", "2"), emptyMap()) }
        // val app6 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "2"), emptyMap()) }

        // when - executing transaction
        try {
            executeChange("http://${peers[0][0]}/create_change")
            fail("Exception not thrown")
        } catch (e: Exception) {
            expect {
                that(e).isA<ServerResponseException>()
                that(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
            }
        }

        // then - transaction should not be executed
        askForChanges("http://${peers[0][2]}")
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        apps.forEach { app -> app.stop() }
    }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit = runBlocking {
        val configOverrides = mapOf(
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${Random.nextInt(5000, 10000) + 11124}" },
                List(5) { "localhost:${Random.nextInt(10001, 20000) + 11134}" }),
            "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
        )

        // given - applications
        val app1 = createApplication(
            arrayOf("1", "1"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 1)
        )
        val app2 = createApplication(
            arrayOf("2", "1"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 1)
        )
        //val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = createApplication(
            arrayOf("1", "2"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 2)
        )
        val app5 = createApplication(
            arrayOf("2", "2"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 2)
        )

        val apps = listOf(app1, app2, app4, app5)
        apps.forEach { app -> app.startNonblocking() }
        val peers = apps.chunked(2).map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
        peers[0].add("localhost:8083")
        peers[1].addAll(listOf("localhost:8086", "localhost:8087", "localhost:8088"))
        apps.forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }

        // val app6 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "2"), emptyMap()) }
        // val app7 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "2"), emptyMap()) }
        // val app8 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("5", "2"), emptyMap()) }

        delay(5000)

        // when - executing transaction
        try {
            executeChange("http://${peers[0][0]}/create_change")
        } catch (e: Exception) {
            expectThat(e).isA<ServerResponseException>()
            expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
        }

        // we need to wait for timeout from peers of second peerset
        delay(10000)

        // then - transaction should not be executed
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isEqualTo(0)
                }
            }

        apps.forEach { app -> app.stop() }
    }

    @Test
    fun `transaction should pass when more than half peers of all peersets are operative`(): Unit = runBlocking {
        val configOverrides = mapOf(
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${Random.nextInt(5000, 10000) + 11124}" },
                List(5) { "localhost:${Random.nextInt(10001, 20000) + 11134}" }),
            "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
        )

        // given - applications
        val app1 = createApplication(
            arrayOf("1", "1"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 1)
        )
        val app2 = createApplication(
            arrayOf("2", "1"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 1)
        )
        //val app3 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("3", "1"), emptyMap()) }

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = createApplication(
            arrayOf("1", "2"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 2)
        )
        val app5 = createApplication(
            arrayOf("2", "2"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 2)
        )
        val app6 = createApplication(
            arrayOf("3", "2"),
            emptyMap(),
            configOverrides = configOverrides,
            mode = TestApplicationMode(3, 2)
        )
        // val app7 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("4", "2"), emptyMap()) }
        // val app8 = GlobalScope.launch(Dispatchers.IO) { startApplication(arrayOf("5", "2"), emptyMap()) }
        val apps = listOf(app1, app2, app4, app5, app6)
        apps.forEach { app -> app.startNonblocking() }
        val peers =
            apps.asSequence()
                .withIndex()
                .groupBy{ it.index < 2 }
                .values
                .map { it.map { it.value } }
                .map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
                .toList()
        peers[0].add("localhost:8083")
        peers[1].addAll(listOf("localhost:8087", "localhost:8088"))
        apps.forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }

        // when - executing transaction
        executeChange("http://${peers[0][0]}/create_change")

        // then - transaction should be executed in every peerset
        askForChanges("http://${peers[0][1]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        askForChanges("http://${peers[1][0]}")
            .let {
                expect {
                    that(it.size).isGreaterThanOrEqualTo(1)
                    that(it[0]).isEqualTo(ChangeWithAcceptNum(AddUserChange("userName"), 1))
                }
            }

        apps.forEach { app -> app.stop() }
    }

    @Test
    fun `transaction should not be processed if every peer from one peerset fails after ft-agree`(): Unit =
        runBlocking {
            val configOverrides = mapOf(
                "raft.server.addresses" to listOf(
                    List(3) { "localhost:${Random.nextInt(5000, 10000) + 11124}" },
                    List(5) { "localhost:${Random.nextInt(10001, 20000) + 11134}" }),
                "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
            )

            val failAction = SignalListener {
                throw RuntimeException()
            }

            // given - applications
            val app1 = createApplication(
                arrayOf("1", "1"),
                configOverrides = configOverrides,
                mode = TestApplicationMode(1, 1)
            )
            val app2 = createApplication(
                arrayOf("2", "1"),
                configOverrides = configOverrides,
                mode = TestApplicationMode(2, 1)
            )
            val app3 = createApplication(
                arrayOf("3", "1"),
                configOverrides = configOverrides,
                mode = TestApplicationMode(3, 1)
            )

            // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
            val app4 = createApplication(
                arrayOf("1", "2"),
                signalListeners = mapOf(Signal.OnHandlingAgreeEnd to failAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(1, 2)
            )
            val app5 = createApplication(
                arrayOf("2", "2"),
                signalListeners = mapOf(Signal.OnHandlingAgreeEnd to failAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(2, 2)
            )
            val app6 = createApplication(
                arrayOf("3", "2"),
                signalListeners = mapOf(Signal.OnHandlingAgreeEnd to failAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(3, 2)
            )
            val app7 = createApplication(
                arrayOf("4", "2"),
                signalListeners = mapOf(Signal.OnHandlingAgreeEnd to failAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(4, 2)
            )
            val app8 = createApplication(
                arrayOf("5", "2"),
                signalListeners = mapOf(Signal.OnHandlingAgreeEnd to failAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(5, 2)
            )
            val apps = listOf(app1, app2, app3, app4, app5, app6, app7, app8)
            apps.forEach { app -> app.startNonblocking() }
            val peers =
                apps.asSequence()
                    .withIndex()
                    .groupBy{ it.index < 3 }
                    .values
                    .map { it.map { it.value } }
                    .map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
                    .toList()
            apps.forEachIndexed { index, application ->
                val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
                application.setOtherPeers(filteredPeers)
            }

            delay(5000)

            // when - executing transaction - should throw too few responses exception
            try {
                executeChange("http://${peers[0][0]}/create_change")
                fail("executing change didn't fail")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too few responses of ft phase.")
            }

            peers.flatten().forEach {
                askForChanges("http://${it}")
                    .let {
                        expectThat(it.size).isEqualTo(0)
                    }
            }

            apps.forEach { app -> app.stop() }
        }

    @Disabled("Delays are still used")
    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit = runBlocking {
        val configOverrides = mapOf(
            "raft.server.addresses" to listOf(
                List(3) { "localhost:${Random.nextInt(5000, 10000) + 11124}" },
                List(5) { "localhost:${Random.nextInt(10001, 20000) + 11134}" }),
            "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
        )

        val failAction = SignalListener {
            throw RuntimeException()
        }

        // given - applications
        val app1 = createApplication(
            arrayOf("1", "1"),
            mapOf(Signal.BeforeSendingApply to failAction),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 1)
        )
        val app2 = createApplication(
            arrayOf("2", "1"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 1)
        )
        val app3 = createApplication(
            arrayOf("3", "1"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(3, 1)
        )

        // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
        val app4 = createApplication(
            arrayOf("1", "2"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(1, 2)
        )
        val app5 = createApplication(
            arrayOf("2", "2"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(2, 2)
        )
        val app6 = createApplication(
            arrayOf("3", "2"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(3, 2)
        )
        val app7 = createApplication(
            arrayOf("4", "2"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(4, 2)
        )
        val app8 = createApplication(
            arrayOf("5", "2"),
            configOverrides = configOverrides,
            mode = TestApplicationMode(5, 2)
        )
        val apps = listOf(app1, app2, app3, app4, app5, app6, app7, app8)
        apps.forEach { app -> app.startNonblocking() }
        val peers =
            apps.asSequence()
                .withIndex()
                .groupBy{ it.index < 3 }
                .values
                .asSequence()
                .map { it.map { it.value } }
                .map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
                .toList()
        apps.forEachIndexed { index, application ->
            val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
            application.setOtherPeers(filteredPeers)
        }

        delay(5000)

        // when - executing transaction something should go wrong after ft-agree
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/create_change")
        }

        // application should elect recovery leader to perform transaction to the end
        delay(20000)

        peers.flatten().forEach {
            askForChanges("http://$it")
                .let {
                    expect {
                        that(it.size).isGreaterThanOrEqualTo(1)
                        that(it[0].change).isEqualTo(AddUserChange("userName"))
                    }
                }
        }

        apps.forEach { app -> app.stop() }
    }

    @Disabled("Delays are still used")
    @Test
    fun `transaction should be processed and should be processed only once when one peerset applies its change and the other not`(): Unit =
        runBlocking {
            val configOverrides = mapOf(
                "raft.server.addresses" to listOf(
                    List(3) { "localhost:${Random.nextInt(5000, 10000) + 11124}" },
                    List(5) { "localhost:${Random.nextInt(10001, 20000) + 11134}" }),
                "raft.clusterGroupIds" to listOf(UUID.randomUUID(), UUID.randomUUID())
            )

            val leaderAction = SignalListener {
                val url2 = "${it.otherPeers[0][0]}/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url2) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT, ChangeDto(
                                mapOf(
                                    "operation" to "ADD_USER",
                                    "userName" to "userName"
                                )
                            )
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("${it.otherPeers[0][0]} sent response to apply")
                val url3 = "${it.otherPeers[0][1]}/apply"
                runBlocking {
                    testHttpClient.post<HttpResponse>(url3) {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                        body = Apply(
                            it.transaction!!.ballotNumber, true, Accept.COMMIT, ChangeDto(
                                mapOf(
                                    "operation" to "ADD_USER",
                                    "userName" to "userName"
                                )
                            )
                        )
                    }.also {
                        println("Got response ${it.status.value}")
                    }
                }
                println("${it.otherPeers[0][1]} sent response to apply")
                throw RuntimeException()
            }

            // given - applications
            val app1 = createApplication(
                arrayOf("1", "1"),
                mapOf(Signal.BeforeSendingApply to leaderAction),
                configOverrides = configOverrides,
                mode = TestApplicationMode(1, 1)
            )
            val app2 = createApplication(
                arrayOf("2", "1"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(2, 1)
            )
            val app3 = createApplication(
                arrayOf("3", "1"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(3, 1)
            )

            // mock of not responding peerset2 - is in config, so transaction leader should wait on their responses
            val app4 = createApplication(
                arrayOf("1", "2"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(1, 2)
            )
            val app5 = createApplication(
                arrayOf("2", "2"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(2, 2)
            )
            val app6 = createApplication(
                arrayOf("3", "2"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(3, 2)
            )
            val app7 = createApplication(
                arrayOf("4", "2"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(4, 2)
            )
            val app8 = createApplication(
                arrayOf("5", "2"),
                emptyMap(),
                configOverrides = configOverrides,
                mode = TestApplicationMode(5, 2)
            )
            val apps = listOf(app1, app2, app3, app4, app5, app6, app7, app8)
            apps.forEach { app -> app.startNonblocking() }
            val peers =
                apps.asSequence()
                    .withIndex()
                    .groupBy{ it.index < 3 }
                    .values
                    .map { it.map { it.value } }
                    .map { it.map { "localhost:${it.getBoundPort()}" } }.toMutableList().map { it.toMutableList() }
                    .toList()
            apps.forEachIndexed { index, application ->
                val filteredPeers = peers.map { peerset -> peerset.filter { it != "localhost:${application.getBoundPort()}" } }
                application.setOtherPeers(filteredPeers)
            }

            delay(5000)

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChange("http://${peers[0][0]}/create_change")
            }

            // application should elect recovery leader to perform transaction to the end
            delay(20000)

            peers.flatten().forEach {
                askForChanges("http://$it")
                    .let {
                        expect {
                            that(it.size).isGreaterThanOrEqualTo(1)
                            that(it[0].change).isEqualTo(AddUserChange("userName"))
                        }
                    }
            }

            apps.forEach { app -> app.stop() }
        }


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
