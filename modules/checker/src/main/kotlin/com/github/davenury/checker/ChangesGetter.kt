package com.github.davenury.checker

import com.github.davenury.common.Changes
import com.github.davenury.common.PeerAddress
import io.ktor.client.request.*
import org.slf4j.LoggerFactory

interface ChangesGetter {
    suspend fun getChanges(address: PeerAddress): Changes
}

class HttpChangesGetter: ChangesGetter {
    // TODO - needs to be changed after multiple peerset support
    override suspend fun getChanges(address: PeerAddress): Changes {
        return try {
            httpClient.get<Changes>("http://${address.address}/v2/change")
        } catch (e: Exception) {
            logger.error("Could not get changes from $address", e)
            throw e
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("HttpChangesGetter")
    }
}