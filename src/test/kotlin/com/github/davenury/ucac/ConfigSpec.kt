package com.github.davenury.ucac

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class ConfigSpec {

    @Test
    fun parsePeers(): Unit = runBlocking {
        val peerAddresses = parsePeers("peer1.com:8080 ,  peer2:8080,  peer3:8080  ; peer4:8080,peer5:8080  ,peer6:8080")
        expectThat(peerAddresses).isEqualTo(
            listOf(
                listOf("peer1.com:8080", "peer2:8080", "peer3:8080"),
                listOf("peer4:8080", "peer5:8080", "peer6:8080"),
            )
        )
    }

}
