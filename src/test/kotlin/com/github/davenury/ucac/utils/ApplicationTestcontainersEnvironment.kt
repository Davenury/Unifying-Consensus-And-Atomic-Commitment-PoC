package com.github.davenury.ucac.utils

import com.github.davenury.common.PeerId
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
    peersetConfiguration: Map<String, List<String>>,
    private val containerPort: Int = 8080,
) : FailureDetectingExternalResource(), Startable {
    companion object {
        private val executor: ExecutorService = Executors.newCachedThreadPool()
    }

    private val peers: MutableMap<PeerId, GenericContainer<*>> = HashMap()
    private val network = Network.newNetwork()

    init {
        val image = ImageFromDockerfile()
            .withDockerfile(TestUtils.getRepoRoot().resolve("Dockerfile"))

        val peerIds = peersetConfiguration.values.flatten().map { PeerId(it) }.toSet()
        val peersConfig = peerIds.joinToString(";") { peerId ->
            "$peerId=${peerNetworkAlias(peerId)}:$containerPort"
        }
        val peersetsConfig = peersetConfiguration.entries.joinToString(";") { (peersetId, peers) ->
            "$peersetId=${peers.joinToString(",")}"
        }

        peerIds.forEach { peerId ->
            val networkAlias = peerNetworkAlias(peerId)
            val redisNetworkAlias = redisNetworkAlias(peerId)
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
                .withEnv("config_peers", peersConfig)
                .withEnv("config_peersets", peersetsConfig)
                .withEnv("config_persistence_type", "REDIS")
                .withEnv("config_persistence_redisHost", redisNetworkAlias)
                .withEnv("config_persistence_redisPort", "6379")
                .withLogConsumer(DockerLogConsumer(networkAlias))
                .waitingFor(Wait.forHttp("/_meta/health"))
                .dependsOn(redisContainer)
            peers[peerId] = container
        }
    }

    private fun peerNetworkAlias(peerId: PeerId) = "peer-$peerId"
    private fun redisNetworkAlias(peerId: PeerId) = "redis-$peerId"

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

    fun getAddress(peerId: String, port: Int = containerPort): String? {
        val host = getHost(peerId)
        val mappedPort = getMappedPort(peerId, port)
        return if (host == null || mappedPort == null) null else "$host:$mappedPort"
    }

    fun getHost(peerId: String): String? = peers[PeerId(peerId)]?.host

    fun getMappedPort(peerId: String, port: Int = containerPort): Int? =
        peers[PeerId(peerId)]?.getMappedPort(port)
}
