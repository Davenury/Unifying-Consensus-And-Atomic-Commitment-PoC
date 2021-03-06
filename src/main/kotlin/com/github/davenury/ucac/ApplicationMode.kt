package com.github.davenury.ucac

sealed class ApplicationMode {
    abstract val port: Int
    abstract val host: String
    abstract val peersetId: Int
    abstract val otherPeers: List<List<String>>
    abstract val nodeId: Int
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
}

object DockerComposeApplicationMode: ApplicationMode() {
    override val host: String
    override val port: Int
    override val peersetId: Int
    override val otherPeers: List<List<String>>
    override val nodeId: Int

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
        val me = config.peers.peersAddresses[_peersetId - 1][id].split(":")[0]

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
            println(
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
    override val peersetId: Int
    override val otherPeers: List<List<String>>
    override val nodeId: Int

    init {
        val config = loadConfig()
        val _peersetId = args[1].toInt()
        val portOffsetFromPreviousPeersets: Int =
            config.peers.peersAddresses.foldIndexed(0) { index, acc, strings ->
                if (index <= _peersetId - 2) acc + strings.size else acc + 0
            }

        host = "localhost"
        port = 8080 + args[0].toInt() + portOffsetFromPreviousPeersets
        peersetId = _peersetId
        otherPeers = getOtherPeers(config, port)
        nodeId = args[0].toInt()
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
            println(
                "Peers addresses doesn't have enough elements in list - peers addresses length: ${config.peers.peersAddresses.size}, index: ${peersetId - 1}"
            )
            throw IllegalStateException()
        }

}