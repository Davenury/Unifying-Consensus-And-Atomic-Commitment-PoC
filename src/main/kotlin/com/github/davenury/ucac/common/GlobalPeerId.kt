package com.github.davenury.ucac.common

/**
 * @author Kamil Jarosz
 */
data class GlobalPeerId(val peersetId: Int, val peerId: Int) {
    override fun toString(): String = "peerset${peersetId}/peer${peerId}"
}
