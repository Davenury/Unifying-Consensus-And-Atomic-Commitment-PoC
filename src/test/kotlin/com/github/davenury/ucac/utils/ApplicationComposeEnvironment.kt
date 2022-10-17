package com.github.davenury.ucac.utils

import org.testcontainers.containers.FailureDetectingExternalResource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.lifecycle.Startable

data class PeerId(val peersetId: Int, val peerId: Int)

/**
 * @author Kamil Jarosz
 */
class ApplicationComposeEnvironment(
    peersets: List<Int>,
    private val containerPort: Int = 8080,
) : FailureDetectingExternalResource(), Startable {
    private val peers: MutableMap<PeerId, GenericContainer<*>> = HashMap()
    private val network = Network.newNetwork()

    init {
        val image = ImageFromDockerfile()
            .withDockerfile(TestUtils.getRepoRoot().resolve("Dockerfile"))
        val peersAddressList = peersets.mapIndexed { index, count ->
            (1..count).map { peerNetworkAlias(index + 1, it) + ":" + containerPort }
        }.joinToString(";") { it.joinToString(",") }

        peersets.forEachIndexed { index, peerCount ->
            val peersetId = index + 1
            (1..peerCount).forEach { peerId ->
                val networkAlias = peerNetworkAlias(peersetId, peerId)
                val container = GenericContainer(image)
                    .withNetworkAliases(networkAlias)
                    .withNetwork(network)
                    .withExposedPorts(containerPort)
                    .withEnv("config_host", networkAlias)
                    .withEnv("config_port", containerPort.toString())
                    .withEnv("config_peerId", peerId.toString())
                    .withEnv("config_peersetId", peersetId.toString())
                    .withEnv("config_peers", peersAddressList)
                    .withLogConsumer(DockerLogConsumer(networkAlias))
                    .waitingFor(Wait.forHttp("/_meta/health"))
                peers[PeerId(peersetId, peerId)] = container
            }
        }
    }

    private fun peerNetworkAlias(peersetId: Int, peerId: Int) = "peer-$peersetId-$peerId"

    override fun start() {
        peers.forEach { (_, v) -> v.start() }
    }

    override fun stop() {
        peers.forEach { (_, v) -> v.stop() }
    }

    fun getHost(peersetId: Int, peerId: Int): String? {
        return peers[PeerId(peersetId, peerId)]?.host
    }

    fun getMappedPort(peersetId: Int, peerId: Int, port: Int = containerPort): Int? {
        return peers[PeerId(peersetId, peerId)]?.getMappedPort(port)
    }
}