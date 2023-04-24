package com.github.davenury.common

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class ConfigSpec {

    @Test
    fun parsePeers() {
        val peerAddresses =
            parsePeers("p1=peer1.com:8080 ; p2= peer2:8080;  p3=peer3:8080  ; p4= peer4:8080;p5=peer5:8080  ;p6=peer6:8080")

        expectThat(peerAddresses).isEqualTo(
            mapOf(
                PeerId("p1") to PeerAddress.of("p1", "peer1.com:8080"),
                PeerId("p2") to PeerAddress.of("p2", "peer2:8080"),
                PeerId("p3") to PeerAddress.of("p3", "peer3:8080"),
                PeerId("p4") to PeerAddress.of("p4", "peer4:8080"),
                PeerId("p5") to PeerAddress.of("p5", "peer5:8080"),
                PeerId("p6") to PeerAddress.of("p6", "peer6:8080"),
            )
        )
    }

    @Test
    fun parsePeersets() {
        val peerAddresses =
            parsePeersets("ps1 = a, b, c;ps2=a,g ,d")

        expectThat(peerAddresses).isEqualTo(
            mapOf(
                PeersetId("ps1") to listOf(PeerId("a"), PeerId("b"), PeerId("c")),
                PeersetId("ps2") to listOf(PeerId("a"), PeerId("g"), PeerId("d")),
            )
        )
    }
}
