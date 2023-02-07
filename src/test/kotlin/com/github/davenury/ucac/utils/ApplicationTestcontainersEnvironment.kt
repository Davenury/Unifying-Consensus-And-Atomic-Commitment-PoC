package com.github.davenury.ucac.utils

import com.github.davenury.ucac.common.GlobalPeerId
import org.testcontainers.containers.FailureDetectingExternalResource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * @author Kamil Jarosz
 */
class ApplicationTestcontainersEnvironment(
    peersets: List<Int>,
    private val containerPort: Int = 8080,
) : FailureDetectingExternalResource(), Startable {
    companion object {
        private val executor: ExecutorService = Executors.newCachedThreadPool()
    }

    private val peers: MutableMap<GlobalPeerId, GenericContainer<*>> = HashMap()
    private val network = Network.newNetwork()

    init {
        val image = ImageFromDockerfile()
            .withDockerfile(TestUtils.getRepoRoot().resolve("Dockerfile"))
        val peersAddressList = peersets.mapIndexed { index, count ->
            (0 until count).map { "${peerNetworkAlias(index, it)}:$containerPort" }
        }.joinToString(";") { it.joinToString(",") }

        peersets.forEachIndexed { peersetId, peerCount ->
            (0 until peerCount).forEach { peerId ->
                val networkAlias = peerNetworkAlias(peersetId, peerId)
                val redisNetworkAlias = redisNetworkAlias(peersetId, peerId)
                val redisContainer = GenericContainer(DockerImageName.parse("redis:7.0-alpine"))
                    .withNetworkAliases(redisNetworkAlias)
                    .withNetwork(network)
                    .withExposedPorts(6379)
                val container = GenericContainer(image)
                    .withNetworkAliases(networkAlias)
                    .withNetwork(network)
                    .withExposedPorts(containerPort)
                    .withEnv("config_host", networkAlias)
                    .withEnv("config_port", containerPort.toString())
                    .withEnv("config_peerId", peerId.toString())
                    .withEnv("config_peersetId", peersetId.toString())
                    .withEnv("config_peers", peersAddressList)
                    .withEnv("config_persistence_type", "REDIS")
                    .withEnv("config_persistence_redisHost", redisNetworkAlias)
                    .withEnv("config_persistence_redisPort", "6379")
                    .withLogConsumer(DockerLogConsumer(networkAlias))
                    .waitingFor(Wait.forHttp("/_meta/health"))
                    .dependsOn(redisContainer)
                peers[GlobalPeerId(peersetId, peerId)] = container
            }
        }
    }

    private fun peerNetworkAlias(peersetId: Int, peerId: Int) = "peer-$peersetId-$peerId"
    private fun redisNetworkAlias(peersetId: Int, peerId: Int) = "redis-$peersetId-$peerId"

    override fun start() {
        peers.values
            .map { executor.submit { it.start() } }
            .forEach { it.get() }
    }

    override fun stop() {
        peers.values
            .map { executor.submit { it.stop() } }
            .forEach { it.get() }
    }

    fun getAddress(peersetId: Int, peerId: Int, port: Int = containerPort): String? {
        val host = getHost(peersetId, peerId)
        val mappedPort = getMappedPort(peersetId, peerId, port)
        return if (host == null || mappedPort == null) null else "$host:$mappedPort"
    }

    fun getHost(peersetId: Int, peerId: Int): String? = peers[GlobalPeerId(peersetId, peerId)]?.host

    fun getMappedPort(peersetId: Int, peerId: Int, port: Int = containerPort): Int? =
        peers[GlobalPeerId(peersetId, peerId)]?.getMappedPort(port)
}
