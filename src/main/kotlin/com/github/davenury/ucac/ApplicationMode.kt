package com.github.davenury.ucac

import org.slf4j.Logger
import org.slf4j.LoggerFactory

sealed class ApplicationMode {
    abstract val port: Int
    abstract val host: String
    abstract val peersetId: Int
    abstract val otherPeers: List<List<String>>
    abstract val nodeId: Int
    abstract val logger: Logger

    abstract fun name(): String
}

class TestApplicationMode(
    override val nodeId: Int,
    override val peersetId: Int,
): ApplicationMode() {
    override val port: Int
        get() = 0
    override val host: String
        get() = "localhost"
    // should be change in application
    override val otherPeers: List<List<String>>
        get() = listOf(listOf())
    override val logger: Logger = LoggerFactory.getLogger(TestApplicationMode::class.java)

    override fun name(): String = "TestApplicationMode"
}

object DockerComposeApplicationMode: ApplicationMode() {
    override val host: String
    override val port: Int
    override val peersetId: Int
    override val otherPeers: List<List<String>>
    override val nodeId: Int
    override val logger: Logger = LoggerFactory.getLogger(DockerComposeApplicationMode::class.java)

    override fun name(): String = "DockerComposeApplicationMode"

    init {
        val _peersetId =
            System.getenv()["PEERSET_ID"]?.toInt()
                ?: throw RuntimeException(
                    "Provide PEERSET_ID env variable to represent id of node"
                )

        val id =
            System.getenv()["RAFT_NODE_ID"]?.toInt()
                ?: throw RuntimeException(
                    "Provide either arg or RAFT_NODE_ID env variable to represent id of node"
                )


        val config = loadConfig()
        val me = config.peers.peersAddresses[_peersetId - 1][id - 1].split(":")[0]

        host = me
        port = 8080
        peersetId = _peersetId
        otherPeers = getOtherPeers(config, me)
        nodeId = id
    }

    private fun getOtherPeers(config: Config, me: String): List<List<String>> =
        try {
            config.peers.peersAddresses.foldIndexed(mutableListOf()) { index, acc, strings ->
                if (index == peersetId - 1) {
                    acc +=
                        strings.filterNot {
                            it.contains(me)
                        }
                    acc
                } else {
                    acc += strings
                    acc
                }
            }
        } catch (e: java.lang.IndexOutOfBoundsException) {
            logger.error(
                "Peers addresses doesn't have enough elements in list - peers addresses length: ${config.peers.peersAddresses.size}, index: ${peersetId - 1}"
            )
            throw IllegalStateException()
        }

}

class LocalDevelopmentApplicationMode(
    args: Array<String>
): ApplicationMode() {
    override val host: String
    override val port: Int
    override val peersetId: Int = args[1].toInt()
    override val otherPeers: List<List<String>>
    override val nodeId: Int = args[0].toInt()
    override val logger: Logger = LoggerFactory.getLogger(LocalDevelopmentApplicationMode::class.java)

    override fun name(): String = "LocalDevelopmentApplicationMode"

    init {
        val config = loadConfig()
        val portOffsetFromPreviousPeersets: Int =
            config.peers.peersAddresses.foldIndexed(0) { index, acc, strings ->
                if (index <= peersetId - 2) acc + strings.size else acc + 0
            }

        host = "localhost"
        port = 8080 + nodeId + portOffsetFromPreviousPeersets
        otherPeers = getOtherPeers(config, port)
    }

    private fun getOtherPeers(config: Config, port: Int): List<List<String>> =
        try {
            config.peers.peersAddresses.foldIndexed(mutableListOf()) { index, acc, strings ->
                if (index == peersetId - 1) {
                    acc +=
                        strings.filterNot {
                            it.contains("$port")
                        }
                    acc
                } else {
                    acc += strings
                    acc
                }
            }
        } catch (e: java.lang.IndexOutOfBoundsException) {
            logger.error(
                "Peers addresses doesn't have enough elements in list - peers addresses length: ${config.peers.peersAddresses.size}, index: ${peersetId - 1}"
            )
            throw IllegalStateException()
        }
}

object KubernetesApplicationMode: ApplicationMode() {
    override val host: String
    override val port: Int
    override val peersetId: Int
    override val otherPeers: List<List<String>>
    override val nodeId: Int

    override fun name(): String = "KubernetesApplicationMode"

    private val allPeers: List<List<String>>

    override val logger: Logger = LoggerFactory.getLogger(KubernetesApplicationMode::class.java)

    init {
        val _peersetId =
            System.getenv()["PEERSET_ID"]?.toInt()
                ?: throw RuntimeException(
                    "Provide PEERSET_ID env variable to represent id of node"
                )

        val id =
            System.getenv()["RAFT_NODE_ID"]?.toInt()
                ?: throw RuntimeException(
                    "Provide either arg or RAFT_NODE_ID env variable to represent id of node"
                )

        val config = loadConfig("application-kubernetes.conf")
        allPeers = System.getenv()["GPAC_PEERS_ADDRESSES"].let { objectMapper.readValue(it, ArrayList<ArrayList<String>>()::class.java) }
        val me = allPeers[_peersetId - 1][id - 1].split(":")[0]

        host = me
        port = 8080
        peersetId = _peersetId
        otherPeers = getOtherPeers(config, me)
        nodeId = id
    }

    private fun getOtherPeers(config: Config, me: String): List<List<String>> =
        try {
            allPeers.foldIndexed(mutableListOf()) { index, acc, strings ->
                if (index == peersetId - 1) {
                    acc +=
                        strings.filterNot {
                            it.contains(me)
                        }
                    acc
                } else {
                    acc += strings
                    acc
                }
            }
        } catch (e: java.lang.IndexOutOfBoundsException) {
            logger.error(
                "Peers addresses doesn't have enough elements in list - peers addresses length: ${config.peers.peersAddresses.size}, index: ${peersetId - 1}"
            )
            throw IllegalStateException()
        }
}

fun determineApplicationMode(args: Array<String>): ApplicationMode {
    val applicationMode = when (System.getenv("application_environment")) {
        "docker_compose" -> DockerComposeApplicationMode
        "k8s" -> KubernetesApplicationMode
        else -> LocalDevelopmentApplicationMode(args)
    }

    applicationMode.logger.info("Deploying application in ${applicationMode.name()} with peers config: ${applicationMode.otherPeers}")

    return applicationMode
}
